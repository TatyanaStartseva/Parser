import sys
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("HOST")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USERNAME_DB")
PASSWORD = os.getenv("PASSWORD_DB")
conn = psycopg2.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)

async def Users(data, cursor):
    try:
        initial_user_data = []
        for key in data["accounts"]:
            user_id = key
            accounts_info = data["accounts"][key]["info"]
            chat_id = list(data["accounts"][key]["chats"].keys())[0]
            if accounts_info.get("username") is not None and accounts_info.get("first_name") is not None:
                username = (
                    accounts_info.get("username").lower()
                )
                initial_user_data.append(
                    (
                        user_id,
                        username,
                        accounts_info.get("bio"),
                        accounts_info.get("first_name"),
                        accounts_info.get("last_name"),
                        accounts_info.get("last_online"),
                        accounts_info.get("premium"),
                        accounts_info.get("phone"),
                        accounts_info.get("image"),
                        chat_id,
                    )
                )

        cursor.executemany(
            "INSERT INTO Users (user_id, username, bio, first_name, last_name, last_online, premium, "
            "phone, image, chat_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (user_id) DO NOTHING ",
            initial_user_data,
        )
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        print(sys.exc_info())


async def Chats(data, cursor):
    initial_data = []
    try:
        for key in data["chats"]:
            chats_key = data["chats"][key]
            chat_id = key
            parent_link = chats_key.get("parent_link")
            children_link = chats_key.get("children_link")
            username = chats_key.get("username")
            title = chats_key.get("title")
            last_online = chats_key.get("last_online")
            initial_data.append((chat_id, parent_link,children_link, username, title, last_online))
        cursor.executemany(
            "INSERT INTO Chats (chat_id, parent_link, children_link, username, title, last_online) VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (chat_id) DO UPDATE SET "
            "username = EXCLUDED.username,parent_link = EXCLUDED.parent_link,children_link = EXCLUDED.children_link, title = EXCLUDED.title, last_online = EXCLUDED.last_online",
            initial_data,
        )
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()


async def Messages(user_data, cursor):
    try:
        initial_data = []
        for key in user_data["accounts"]:
            for key_chat in user_data["accounts"][key]["chats"]:
                accounts_info = user_data["accounts"][key]["info"]
                for message_data in user_data["accounts"][key]["chats"][key_chat]:
                    if accounts_info.get("username") is not None and accounts_info.get("first_name") is not None:
                        message_id = message_data["message_id"]
                        text = message_data["text"]
                        user_id = int(key)
                        chat_id = int(key_chat)
                        initial_data.append((message_id, text, user_id, chat_id))
        cursor.executemany(
            "INSERT INTO Messages (message_id, message, user_id, chat_id) VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (message_id) DO UPDATE SET "
            "message = EXCLUDED.message, user_id = EXCLUDED.user_id, chat_id = EXCLUDED.chat_id",
            initial_data,
        )
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()


async def background_save(data):
    try:
        cursor = conn.cursor()
        await Chats(data, cursor)
        await Users(data, cursor)
        await Messages(data, cursor)
    except Exception as e:
        print(f"Error: {e}")
        print(sys.exc_info())
    finally:
        cursor.close()
        conn.close()
