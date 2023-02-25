import asyncio
import tweepy
import math
import os
import logging 
import networkx as nx 
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

client = TelegramClient('samkazbot', TELEGRAM_API_ID, TELEGRAM_API_HASH)

graph = nx.DiGraph()

async def process_telegram_message(msg: Message, child_node_id=None, added_nodes=None):
    # Creates graph nodes for current message and all parent messages (recursively)
    # returns the list of all message ids that were added to the graph. 
    # This list is ordered from oldest to newest.
    if not added_nodes: 
        added_nodes = []
    _id = msg.id 
    if graph.has_node(_id):
        logger.info(f"Message {_id} already processed") 
        return 
    if type(msg.message) != str:
        raise Exception("Expected message to be string.")
    # Add current node and all parent nodes to graph 
    graph.add_node(_id, msg=msg.message)
    added_nodes.append(_id)
    if child_node_id:
        graph.add_edge(_id, child_node_id)
    msg_parent = await msg.get_reply_message()
    if msg_parent:
        # Will be defined if msg has a parent message (i.e. msg is reply to msg_parent)
        # Recursively process parent message(s)
        await process_telegram_message(msg_parent, child_node_id=_id, added_nodes=added_nodes)
    # Must reverse the order before returning so that oldest messages are first. 
    return list(reversed(added_nodes))

async def main():
    await client.start(PHONE_NUMBER, TELEGRAM_2FA_CODE)
    print("Client started")
    fraximalists = "https://t.me/fraxfinance"
    frax_jesus = await client.get_entity("samkazemian")

    @client.on(events.NewMessage(
        chats=[fraximalists], 
        incoming=True, 
        # from_users=frax_jesus, 
    ))
    async def handle_message_from_god(msg):
        added_nodes = await process_telegram_message(msg)
        # print(added_nodes)

    msgs = await client.get_messages(fraximalists, limit=25, from_user=frax_jesus)
    for msg in msgs:
        added_nodes = await process_telegram_message(msg)



        print(added_nodes)

    

    import matplotlib.pyplot as plt
    nx.drawing.draw_networkx(graph)
    plt.show()

    # await client.run_until_disconnected()

if __name__ ==  '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

# Saving to Google Spreadsheet

# gc = gspread.service_account_from_dict(GOOGLE_CRED)
# sh = gc.open_by_url(url=GOOGLE_SHEET_URL)


# client2 = tweepy.Client(
#     consumer_key=API_KEY, consumer_secret=API_KEY_SECRET,
#     access_token=ACCESS_TOKEN, access_token_secret=ACCESS_TOKEN_SECRET
# )


# async def divide_post(message):
#     if len(message) > 280:
#         parts = math.ceil(len(message) / 280)
#         message = message.split()

#         messagelength = math.ceil(len(message) / parts)
#         chunks = [
#             message[i: i + messagelength]
#             for i in range(0, len(message), messagelength)
#         ]
#         message_chunks = []
#         for i, j in enumerate(chunks):
#             message_chunks.append(" ".join(j).strip() + f" {i + 1}/{len(chunks)}")
#         return message_chunks
#     else:
#         return [message]


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