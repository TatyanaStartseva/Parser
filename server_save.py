import asyncio
import sys
from aiohttp import web
from pymongo import MongoClient
from retry import retry


cluster = MongoClient("mongodb+srv://tatyanastartseva2020:IIr08PTqUBbyK0Jq@parserdb.gwhsqxg.mongodb.net/Parserdb")
db = cluster["Parserdb"]

async def retry(func_name, *args, **kwargs):
    async def _wrapper():
        while True:
            try:
                await func_name(*args, **kwargs)
                break
            except Exception as e:
                print(f"Произошла ошибка: {e}. Повторная попытка...")
                await asyncio.sleep(1)
        else:
            raise Exception("Сбой после нескольких попыток")
    await _wrapper()

async def insert_or_update_one(collection, query, update=None):
    try:
        result = collection.update_one(query, {'$set': update}, upsert=True)
        return result
    except Exception as e:
        raise e

async def Users(data):
    try:
        collection = db["Users"]
        for key in data['accounts']:
            exist_user = collection.find_one({'user_id': key})
            username = None
            if data['accounts'][key]['info'].get('username') is not None:
                username = data['accounts'][key]['info'].get('username').lower()
            update = {
                'user_id': key,
                'username': username,
                'first_name': data['accounts'][key]['info'].get('first_name'),
                'last_name': data['accounts'][key]['info'].get('last_name'),
                'last_online': data['accounts'][key]['info'].get('last_online'),
                'premium': data['accounts'][key]['info'].get('premium'),
                'phone': data['accounts'][key]['info'].get('phone'),
                'image': data['accounts'][key]['info'].get('image'),
            }
            if exist_user is not None:
                update['past_first_name'] = exist_user.get('first_name')
                update['past_last_name'] = exist_user.get('last_name')

            query = {'user_id': key}
            await retry(insert_or_update_one, collection, query, update)
    except Exception as e:
        print(f'Error: {e}')
        print(sys.exc_info())

async def Messages(user_data):
    try:
        collection = db["Messages"]
        for key in user_data['accounts']:
            for key_chat in user_data['chats']:
                for messages in user_data['accounts'][key]['chats']:
                    for i in range(len(user_data['accounts'][key]['chats'][messages])):
                        message_id = user_data['accounts'][key]['chats'][messages][i]['message_id']
                        text = user_data['accounts'][key]['chats'][messages][i]['text']
                        exist_message = collection.find_one({'message_id': message_id})
                        if exist_message is None:
                            await retry(insert_or_update_one, collection, {'message_id': message_id, 'user_id': key, 'chat_id': key_chat, 'text': text})
    except Exception as e:
        print(f'Error: {e}')
        print(sys.exc_info())

async def Chats(data):
    try:
        collection = db["Chats"]
        for key in data['chats']:
            exist_chat = collection.find_one({'chat_id': key})
            update = {
                'chat_id': key,
                'username': data['chats'][key].get('username'),
                'title': data['chats'][key].get('title'),
                'last_online': data['chats'][key].get('last_online')
            }
            if exist_chat is not None:
                update['past_title'] = exist_chat.get('title')

            query = {'chat_id': key}
            await retry(insert_or_update_one, collection, query, update)
    except Exception as e:
        print(f'Error: {e}')
        print(sys.exc_info())

async def background_save(user_data):
    try:
        await Chats(user_data)
        await Users(user_data)
        await Messages(user_data)
        return web.Response(text="Запрос выполнен.")
    except Exception as e:
        print(f'Error: {e}')
        print(sys.exc_info())
