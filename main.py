import asyncio
import tweepy
import math
import os
import re 
import json 
import logging 
import networkx as nx 
import gspread
from pathlib import Path 
from concurrent.futures import ThreadPoolExecutor
from telethon import TelegramClient, sync
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch, InputChannel
from telethon import events
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty
from telethon.utils import get_display_name, get_peer, get_input_user

from telethon.tl.custom.message import Message
import gspread

from dotenv import load_dotenv

assert load_dotenv('.ENV')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("samkazbot")

PHONE_NUMBER = os.environ['PHONE_NUMBER']
TELEGRAM_2FA_CODE = os.environ['TELEGRAM_2FA_CODE']
TELEGRAM_API_ID = os.environ['TELEGRAM_API_ID']
TELEGRAM_API_HASH = os.environ['TELEGRAM_API_HASH']
PATH_TO_GSPREAD_SERVICE_ACCOUNT = os.environ['PATH_TO_GSPREAD_SERVICE_ACCOUNT']
GSPREAD_URL = os.environ['GSPREAD_URL']

# Setup connection to google sheet 
COLS = ["telegram_msg_id", "tweet_status", "telegram_msg", "tweet_msg"]
gc_creds_json = None 
with Path(PATH_TO_GSPREAD_SERVICE_ACCOUNT).open('r') as f:
    gc_creds_json = json.loads(f.read())
gc = gspread.service_account_from_dict(gc_creds_json)
sh = gc.open_by_url(url=GSPREAD_URL)
print("Google spreadsheet connection established")

client = TelegramClient('samkazbot', TELEGRAM_API_ID, TELEGRAM_API_HASH)

graph = nx.DiGraph()

async def process_telegram_message(msg: Message, samkaz_user_id, child_node_id=None, added_msg_ids=None):
    # Creates graph nodes for current message and all parent messages (recursively)
    # returns the list of all message ids that were added to the graph. 
    # This list is ordered from oldest to newest.
    if not added_msg_ids: 
        added_msg_ids = []
    _id = msg.id 
    if graph.has_node(_id):
        logger.info(f"Message {_id} already processed") 
        return 
    if type(msg.message) != str:
        raise Exception("Expected message to be string.")
    # Add current node and all parent nodes to graph 
    # print("message", msg)
    graph.add_node(_id, msg=msg.message, is_samkaz=msg.from_id.user_id == samkaz_user_id)
    added_msg_ids.append(_id)
    if child_node_id:
        graph.add_edge(_id, child_node_id)
    msg_parent = await msg.get_reply_message()
    if msg_parent:
        # Will be defined if msg has a parent message (i.e. msg is reply to msg_parent)
        # Recursively process parent message(s)
        await process_telegram_message(msg_parent, samkaz_user_id, child_node_id=_id, added_msg_ids=added_msg_ids)
    # Must reverse the order before returning so that oldest messages are first. 
    return list(reversed(added_msg_ids))

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
    
# statuses 
# STATUS_TWEET_PENDING: Tweet is in the process of being tweeted. 
# STATUS_TWEETED: Tweet exists on twitter. 

def is_empty_row(row): 
    for prop in COLS:
        if row[prop] != '':
            return False
    return True

async def publish_tweets():
    # Publishes tweets that have not been published yet. 
    # Runs on a cron schedule. 
    import datetime 
    print("Publishing tweets...", datetime.datetime.now())
    # Get all message ids from the graph. Note: This must be in topo-sort order 
    all_msg_ids = list(nx.topological_sort(graph)) 

    # Get the existing list of published tweets 
    published_ids = set()
    for row in sh.sheet1.get_all_records():
        if is_empty_row(row):
            continue
        telegram_msg_id = row['telegram_msg_id']	
        tweet_status = row['tweet_status']
        if tweet_status == 'TWEETED': 
            published_ids.add(telegram_msg_id)

    # Clear all pending tweets from the spreadsheet
    criteria_re = re.compile(r'^STATUS_TWEET_PENDING')
    cell_list = sh.sheet1.findall(criteria_re)
    print(f"Removing {len(cell_list)} pending tweets from the spreadsheet")
    sh.sheet1.batch_clear([f"A{cell.row}:Z{cell.row}" for cell in cell_list])

    # Determine which tweets are unpublished
    unpublished_ids = set(all_msg_ids).difference(published_ids)

    # All unpublished tweets should be published 
    new_rows = []
    for _id in all_msg_ids: 
        if _id in unpublished_ids:
            # Mark tweet as pending in the database 
            telegram_msg = graph.nodes[_id]['msg']
            is_samkaz = graph.nodes[_id]['is_samkaz']
            tweet_msgs = divide_post(telegram_msg, is_samkaz)
            for tweet_msg in tweet_msgs:
                new_rows.append({
                    "telegram_msg_id": _id, 
                    "tweet_status": "STATUS_TWEET_PENDING", 
                    "telegram_msg": telegram_msg, 
                    "tweet_msg": tweet_msg
                })

    # Add the new rows to the spreadsheet to indicate the tweets are pending. 




# Start the scheduling loop
async def publish_tweets_loop(secs: int = 5):
    while True: 
        asyncio.create_task(publish_tweets())
        await asyncio.sleep(secs)


async def main():
    # Start the telegram client
    await client.start(PHONE_NUMBER, TELEGRAM_2FA_CODE)
    print("Client started")
    fraximalists = "https://t.me/fraxfinance"
    frax_jesus = await client.get_entity("samkazemian")
    samkaz_user_id = frax_jesus.id

    # Each time there is a new message, we update our graph 
    # with all relevant messages that must be turned into tweets 
    # @client.on(events.NewMessage(
    #     chats=[fraximalists], 
    #     incoming=True, 
    #     from_users=frax_jesus, 
    # ))
    # async def handle_message_from_god(msg):
    #     # Get ids of all messages added to the graph 
    #     logger.info(f"Handling a new message from samkazemian")
    #     added_msg_ids = await process_telegram_message(msg.message, samkaz_user_id)
    #     print("Added messages to graph: ", added_msg_ids)
    
    # Starts the async background task that publishes tweets
    asyncio.create_task(publish_tweets_loop())

    msgs = await client.get_messages(fraximalists, limit=3, from_user=frax_jesus)
    for msg in msgs:
        # Add relevant nodes to the graph
        added_msg_ids = await process_telegram_message(msg, samkaz_user_id)
        # For each relevant node, determine if we have tweeted yet. 
        print(added_msg_ids)

    # Runs the telegram client until disconnect occurs 
    await client.run_until_disconnected()


    # import matplotlib.pyplot as plt
    # nx.drawing.draw_networkx(graph)
    # plt.show()

    # await client.run_until_disconnected()

if __name__ ==  '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())



# client2 = tweepy.Client(
#     consumer_key=API_KEY, consumer_secret=API_KEY_SECRET,
#     access_token=ACCESS_TOKEN, access_token_secret=ACCESS_TOKEN_SECRET
# )





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