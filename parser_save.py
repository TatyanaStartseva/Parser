import asyncio
import os
import sys

from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

cluster = MongoClient(os.getenv("MONGODB_URI"))
db = cluster[os.getenv("DATABASE_NAME")]


async def retry(func_name, *args, **kwargs):
    async def _wrapper():
        while True:
            try:
                await func_name(*args, **kwargs)
                break
            except Exception as e:
                print(f"Произошла ошибка: {e}. Повторная попытка...")
                await asyncio.sleep(1)

    await _wrapper()


async def insert_or_update_one(collection, query, update=None):
    try:
        return collection.update_one(query, {"$set": update}, upsert=True)
    except Exception as e:
        raise e


async def Users(data):
    try:
        collection = db["Users"]
        for key in data["accounts"]:
            accounts_info = data["accounts"][key]["info"]
            username = None
            if accounts_info.get("username") is not None:
                username = accounts_info.get("username").lower()
            update = {
                "user_id": key,
                "username": username,
                "bio": accounts_info.get("bio"),
                "first_name": accounts_info.get("first_name"),
                "last_name": accounts_info.get("last_name"),
                "last_online": accounts_info.get("last_online"),
                "premium": accounts_info.get("premium"),
                "phone": accounts_info.get("phone"),
                "image": accounts_info.get("image"),
            }

            await retry(insert_or_update_one, collection, {"user_id": key}, update)
    except Exception as e:
        print(f"Error: {e}")
        print(sys.exc_info())


async def Messages(user_data):
    try:
        collection = db["Messages"]
        for key in user_data["accounts"]:
            for key_chat in user_data["chats"]:
                for messages in user_data["accounts"][key]["chats"]:
                    for message_data in user_data["accounts"][key]["chats"][messages]:
                        message_id = message_data["message_id"]
                        text = message_data["text"]
                        await retry(
                            insert_or_update_one,
                            collection,
                            {"message_id": message_id},
                            {
                                "message_id": message_id,
                                "user_id": key,
                                "chat_id": key_chat,
                                "text": text,
                            },
                        )

    except Exception as e:
        print(f"Error: {e}")
        print(sys.exc_info())


async def Chats(data):
    try:
        collection = db["Chats"]

        for key in data["chats"]:
            chats_key = data["chats"][key]

            update = {
                "chat_id": key,
                "username": chats_key.get("username"),
                "title": chats_key.get("title"),
                "last_online": chats_key.get("last_online"),
            }

            await retry(insert_or_update_one, collection, {"chat_id": key}, update)
    except Exception as e:
        print(f"Error: {e}")
        print(sys.exc_info())


async def background_save(user_data):
    try:
        await Chats(user_data)
        await Users(user_data)
        await Messages(user_data)
    except Exception as e:
        print(f"Error: {e}")
        print(sys.exc_info())
