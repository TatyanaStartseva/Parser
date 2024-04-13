import sys
import asyncpg
import asyncio
import os
from dotenv import load_dotenv
import datetime

load_dotenv()
HOST = os.getenv("HOST")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USERNAME_DB")
PASSWORD = os.getenv("PASSWORD_DB")


async def connect_to_database():
    while True:
        try:
            return await asyncpg.create_pool(
                host=HOST,
                database=DATABASE,
                user=USER,
                password=PASSWORD
            )
        except asyncpg.Error as e:
            print(f"Error connecting to the database: {e}")
            print("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


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


async def insert_or_update_one(pool, table_name, fields, updates):
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                    fields_str = ", ".join(fields)
                    placeholders = ", ".join(["$" + str(i+1) for i in range(len(fields))])
                    insert_query = f"INSERT INTO {table_name} ({fields_str}) VALUES ({placeholders}) ON CONFLICT ({fields[0]}) DO UPDATE SET "
                    update_fields = [f"{field} = EXCLUDED.{field}" for field in updates.keys()]
                    update_query = insert_query + ", ".join(update_fields)
                    values = list(updates.values())
                    if table_name == "user_chat":
                        select_query = f"SELECT * FROM {table_name} WHERE user_id = $1 AND chat_id = $2"
                        existing_record = await conn.fetchrow(select_query, updates["user_id"], updates["chat_id"])
                    elif table_name == "messages":
                        select_query = f"SELECT * FROM {table_name} WHERE user_id = $1 AND chat_id = $2 AND message_id=$3"
                        existing_record = await conn.fetchrow(select_query, updates["user_id"], updates["chat_id"],
                                                              updates["message_id"])
                    else:
                        await conn.execute(update_query, *values)
                        return
                    if not existing_record:
                        await conn.execute(insert_query, *values)
    except Exception as e:
        raise e


async def Users(data, pool):
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for key in data["accounts"]:
                    accounts_info = data["accounts"][key]["info"]
                    chat_id = list(data["accounts"][key]["chats"].keys())[0]
                    if (
                        accounts_info.get("username") is not None
                        and accounts_info.get("first_name") is not None
                    ):
                        username = accounts_info.get("username").lower()
                        last_online = datetime.datetime.strptime(accounts_info.get("last_online"), '%Y-%m-%d %H:%M:%S') if accounts_info.get("last_online") is not None else None
                        update = {
                            "user_id": key,
                            "username": username,
                            "bio": accounts_info.get("bio"),
                            "first_name": accounts_info.get("first_name"),
                            "last_name": accounts_info.get("last_name"),
                            "last_online": last_online,
                            "premium": accounts_info.get("premium"),
                            "phone": accounts_info.get("phone"),
                            "image": accounts_info.get("image"),
                        }
                        update_user_chat = {
                            "user_id": key,
                            "chat_id": chat_id,
                        }
                        await retry(
                            insert_or_update_one,
                            pool,
                            "users",
                            [
                                "user_id",
                                "username",
                                "bio",
                                "first_name",
                                "last_name",
                                "last_online",
                                "premium",
                                "phone",
                                "image",
                            ],
                            update,
                        )
                        await retry(
                            insert_or_update_one,
                            pool,
                            "user_chat",
                            ["user_id", "chat_id"],
                            update_user_chat,
                        )
    except Exception as e:
        print(f"Error: {e}")
        print(sys.exc_info())


async def Chats(data, pool):
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for key in data["chats"]:
                    chats_key = data["chats"][key]
                    last_online = datetime.datetime.strptime(chats_key.get("last_online"),'%Y-%m-%d %H:%M:%S') if chats_key.get("last_online") is not None else None
                    update = {
                        "chat_id": key,
                        "parent_link": chats_key.get("parent_link"),
                        "children_link": chats_key.get("children_link"),
                        "title": chats_key.get("title"),
                        "last_online": last_online,
                    }
                    await retry(
                        insert_or_update_one,
                        pool,
                        "chats",
                        ["chat_id", "parent_link", "children_link", "title", "last_online"],
                        update,
                    )
    except Exception as e:
        print(f"Error: {e}")


async def Messages(user_data, pool):
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for key in user_data["accounts"]:
                    for key_chat in user_data["accounts"][key]["chats"]:
                        accounts_info = user_data["accounts"][key]["info"]
                        for message_data in user_data["accounts"][key]["chats"][key_chat]:
                            if (
                                accounts_info.get("username") is not None
                                and accounts_info.get("first_name") is not None
                            ):
                                update = {
                                    "message_id": message_data["message_id"],
                                    "message": message_data["text"],
                                    "user_id": int(key),
                                    "chat_id": int(key_chat),
                                }
                                await retry(
                                    insert_or_update_one,
                                    pool,
                                    "messages",
                                    ["message_id", "message", "user_id", "chat_id"],
                                    update,
                                )
    except Exception as e:
        print(f"Error: {e}")


async def background_save(data):
    try:
        pool = await connect_to_database()
        await Chats(data, pool)
        await Users(data, pool)
        await Messages(data, pool)
    except Exception as e:
        print(f"Error: {e}")
        print(sys.exc_info())
    finally:
        await pool.close()