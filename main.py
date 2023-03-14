import asyncio
import tweepy
import os
import re 
import logging 
import networkx as nx 
import datetime as dt 
import matplotlib.pyplot as plt
from collections import defaultdict
from typing import List 
from datetime import datetime 
from pathlib import Path 
from google.cloud import ndb


from telethon.tl.custom.message import Message
import gspread
from telethon import TelegramClient

from dotenv import load_dotenv

assert load_dotenv('.ENV')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("samkazbot")

class TelegramTweetLink(ndb.Model):
    # Maps a telegram message to a tweet id, one entity will exist per published tweet. 
    # There is a one to many mapping between telegram messages and tweets 
    tg_msg_id = ndb.IntegerProperty()
    tg_usr_id = ndb.IntegerProperty()
    tg_msg = ndb.StringProperty()
    tweet_id = ndb.IntegerProperty()
    tweet_msg = ndb.StringProperty()
    parent_tweet_id = ndb.IntegerProperty()

# environment setup 
PHONE_NUMBER = os.environ['PHONE_NUMBER']
TELEGRAM_2FA_CODE = os.environ['TELEGRAM_2FA_CODE']
TELEGRAM_API_ID = os.environ['TELEGRAM_API_ID']
TELEGRAM_API_HASH = os.environ['TELEGRAM_API_HASH']
GOOGLE_APPLICATION_CREDENTIALS = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
TWITTER_CLIENT_ID = os.environ['TWITTER_CLIENT_ID']
TWITTER_CLIENT_SECRET = os.environ['TWITTER_CLIENT_SECRET']
TWITTER_ACCESS_TOKEN = os.environ['TWITTER_ACCESS_TOKEN']
TWITTER_ACCESS_TOKEN_SECRET = os.environ['TWITTER_ACCESS_TOKEN_SECRET']
TWITTER_BEARER_TOKEN = os.environ['TWITTER_BEARER_TOKEN']

client = TelegramClient('samkazbot', TELEGRAM_API_ID, TELEGRAM_API_HASH)

twitter = tweepy.Client(
    consumer_key=TWITTER_CLIENT_ID, 
    consumer_secret=TWITTER_CLIENT_SECRET,
    access_token=TWITTER_ACCESS_TOKEN, 
    access_token_secret=TWITTER_ACCESS_TOKEN_SECRET, 
    # bearer_token=TWITTER_BEARER_TOKEN, 
)

db = ndb.Client()

@ndb.transactional()
def put_ttls(ttls: List[TelegramTweetLink]):
    # All or nothing db bulk write
    for ttl in ttls: 
        ttl.put()

def plot_digraph(graph):
    nx.drawing.draw_networkx(graph)
    plt.show() 

async def build_tg_graph(graph: nx.DiGraph, msg: Message, tg_user_id_samk, child_node_id=None):
    if graph.has_node(msg.id):
        logger.info(f"Message {msg.id} already processed") 
        return 
    if type(msg.message) != str:
        raise Exception("Expected message to be string.")
    # Add current node and all parent nodes to graph 
    logger.info(f"Message {msg.id} processed!") 
    graph.add_node(
        msg.id, 
        user_id=msg.from_id.user_id, 
        msg=msg.message, 
        is_samkaz=msg.from_id.user_id == tg_user_id_samk
    )
    if child_node_id:
        graph.add_edge(msg.id, child_node_id)
    msg_parent = await msg.get_reply_message()
    if msg_parent:
        # Will be defined if msg has a parent message (i.e. msg is reply to msg_parent)
        # Recursively process parent message(s)
        await build_tg_graph(graph, msg_parent, tg_user_id_samk, child_node_id=msg.id)


def divide_string(string, n):
    return [string[i:i+n] for i in range(0, len(string), n)]

# 260 so we can fit in the name of the user. 
def divide_post(message: str, is_samkaz: bool, chunk_size=260):
    chunks = []
    if len(message) <= chunk_size:
        chunks.append(message)
    else: 
        delimeter = ' '
        chunk = ''
        tokens = message.split(delimeter)
        for token in tokens: 
            if len(token) > chunk_size: 
                # token is too long to fit in a single chunk, so we must split it into multiple chunks 
                token_chunks = divide_string(token, chunk_size)
                chunks.extend(token_chunks)
            else: 
                # token itself can fit within chunk 
                pending_chunk = chunk + delimeter + token 
                if len(pending_chunk) <= chunk_size:
                    # successfully added the current token to the current chunk 
                    chunk = pending_chunk
                else: 
                    # cannot add current token to current chunk, so we must add the current chunk to the list of chunks and token to new chunk. 
                    chunks.append(chunk)
                    chunk = token
        chunks.append(chunk)

    return [
        f"[{'samkazemian' if is_samkaz else 'anon'} {i+1} / {len(chunks)}] {chunk}" for i, chunk in enumerate(chunks)
    ]


async def main():
    now = datetime.now(dt.timezone.utc)

    # Start the telegram client
    await client.start(PHONE_NUMBER, TELEGRAM_2FA_CODE)
    print(f"Telegram client started at {now.isoformat()}")
    fraximalists = "https://t.me/fraxfinance"
    tg_user_samk = await client.get_entity("samkazemian")
    tg_user_id_samk = tg_user_samk.id

    # Retrieve twitter user for bot 
    tw_user_samkbot = twitter.get_user(username="samkazbot", user_auth=True)
    tw_user_id_samkbot = tw_user_samkbot.data['id']

    # Determine the last of sam's telegram messages that we published a tweet for
    limit = 10
    kwargs = {}
    with db.context(): 
        last_tg_msg_from_sam = (
            TelegramTweetLink
                .query()
                .filter(TelegramTweetLink.tg_usr_id == tg_user_id_samk)
                .order(-TelegramTweetLink.tg_msg_id)
                .fetch(1)
        )
    if last_tg_msg_from_sam:
        max_id_found = last_tg_msg_from_sam.pop().tg_msg_id
        print(f"Located telegram-tweet-link in database. Max id found is: {max_id_found}")
        # It is critical that both ids are set, so all messages in range are retrieved. see docs for get_messages for rationale. 
        kwargs = { "min_id": max_id_found, "max_id": 2147483647 } # max possible id 
    else: 
        print("Application is starting for the first time")
        kwargs = { "limit": limit }

    # Start loop to retrieve telegram messages 
    tg_msg_graph = None 
    poll_interval = 5 
    while True: 
        
        # 1. Get recent messages from samk on telegram. 
        # case 1: If app is starting for the first time, we retrieve his last `limit` messages. 
        # case 2: If app is restarting, we retrieve all messages after the last message we published a tweet for.
        tg_msgs_from_sam = await client.get_messages(fraximalists, from_user=tg_user_id_samk, **kwargs)
        print(f"Retrieved {len(tg_msgs_from_sam)} messages from samkazemian")
        
        # 2. Build a directed graph containing all messages from samkazemian + all ancestor messages to sam's messages. 
        tg_msg_graph = nx.DiGraph()        
        for new_msg in tg_msgs_from_sam:
            await build_tg_graph(tg_msg_graph, new_msg, tg_user_id_samk)

        # 3. Determine what telegram messages need to be converted to tweets 
        print(f"There are {len(tg_msg_graph.nodes)} total messages linked to this set of messages from samkazemian")
        min_tg_id = min(tg_msg_graph.nodes)
        print(f"The minimum telegram msg id in this set is: {min_tg_id}")
        with db.context(): 
            ttls = (
                TelegramTweetLink
                    .query()
                    .filter(TelegramTweetLink.tg_msg_id >= min_tg_id)
                    .fetch()
            )
        
        # Publish tweets 
        counter = 0
        tid_to_tweet_id = defaultdict(list)
        for tid in nx.topological_sort(tg_msg_graph):
            node = tg_msg_graph.nodes[tid]
            predecessors = list(tg_msg_graph.predecessors(tid))
            if len(predecessors) > 1: 
                raise Exception('well this is bad lol')
            parent = (predecessors and predecessors.pop()) or None
            parent_tweet_id = tid_to_tweet_id[parent].pop() if parent else None
            tweeted = any(ttl.tg_msg_id == tid for ttl in ttls)
            if not tweeted:
                node = tg_msg_graph.nodes[tid]
                print(f"Creating tweet(s) for telegram message with id: {tid}")
                msgs = divide_post(node['msg'], node['is_samkaz'])
                for msg in msgs:
                    print(f"- {msg}")
                    tweet_id = counter
                    
                    kwargs = {}
                    if parent_tweet_id:
                        kwargs['in_reply_to_tweet_id'] = parent_tweet_id
                    tweet = twitter.create_tweet(text=msg, **kwargs, user_auth=True)
                    tweet_id = tweet.data.id 
                    
                    print(tweet_id)
                    raise Exception("fuck")
                
                    tid_to_tweet_id[tid].append(tweet_id)
                    # Create db artifact referencing the tweet 
                    ttl = TelegramTweetLink(
                        tg_msg_id = tid, 
                        tg_usr_id = node['user_id'],
                        tg_msg = node['msg'],
                        tweet_id = tweet_id,
                        tweet_msg = msg, 
                        parent_tweet_id = parent_tweet_id, 
                    )
                    ttls.append(ttl)

                    



        # Update the database 
        with db.context():
            put_ttls(ttls)



        await asyncio.sleep(poll_interval)






    # print(samkazbot_id)
    # public_tweets = twitter.get_users_tweets(samkazbot_id)
    # print(public_tweets.data)
    # for tweet in public_tweets.data or []:
    #     print(type(tweet), tweet)

    # Each time there is a new message, we update our graph 
    # with all relevant messages that must be turned into tweets 
    # @client.on(events.NewMessage(
    #     chats=[fraximalists], 
    #     incoming=True, 
    #     from_users=samk, 
    # ))
    # async def handle_message_from_god(msg):
    #     # Get ids of all messages added to the graph 
    #     logger.info(f"Handling a new message from samkazemian")
    #     added_msg_ids = await process_telegram_message(msg.message, samkaz_user_id)
    #     print("Added messages to graph: ", added_msg_ids)
    
    # Starts the async background task that publishes tweets
    # asyncio.create_task(publish_tweets_loop())

    # msgs = await client.get_messages(fraximalists, limit=3, from_user=samk, )
    # for msg in msgs:
    #     # Add relevant nodes to the graph
    #     added_msg_ids = await process_telegram_message(msg, samkaz_user_id)
        # For each relevant node, determine if we have tweeted yet. 
    #     print(added_msg_ids)

    # # Runs the telegram client until disconnect occurs 
    # await client.run_until_disconnected()

    import matplotlib.pyplot as plt
    nx.drawing.draw_networkx(tg_msg_graph)
    plt.show()

    # await client.run_until_disconnected()

if __name__ ==  '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


# async def post_tweet(message, reply=None):
#     return client2.create_tweet(text=message, in_reply_to_tweet_id=reply)


# async def sending_tweet(msg1, msg2):
#     message = "Anon: " + msg1
#     previous_message = None
#     msg = await divide_post(message)
#     for i in msg:
#         previous_message = await post_tweet(i, previous_message)
#         previous_message = previous_message.data['id']
#     message = "Sam: " + msg2
#     msg = await divide_post(message)
#     for i in msg:
#         previous_message = await post_tweet(i, previous_message)
#         previous_message = previous_message.data['id']


# async def save_to_spreadsheet(message, reply):
#     sh.sheet1.append_row(values=[message, reply], value_input_option='USER_ENTERED')
#     print(sh.sheet1.get())


# Constantly checking for User's message and forward it if found

# @client.on(events.NewMessage(chats=group, incoming=True, from_users=user))
# async def handler(event):
#     if event.is_group:
#         # try:
#             if event.is_reply:
#                 original_message = event.raw_text
#                 print(original_message)
#                 reply_id = event.message.reply_to
#                 msg_id = reply_id.reply_to_msg_id
#                 reply = await client.get_messages(event.chat, ids=msg_id)
#                 reply_message = reply.message
#                 print(reply_message)

#                 await save_to_spreadsheet(original_message, reply_message)
#                 await sending_tweet(reply_message, original_message)
#         # print(username)

#         # if time.time() - now > 30:
#         #     group_checker()
#         # except:
#         #     print("Error")


# client.run_until_disconnected()