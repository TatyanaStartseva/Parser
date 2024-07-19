import sys
import asyncpg
import asyncio
import os
from dotenv import load_dotenv
import datetime
import logging

load_dotenv()
HOST = os.getenv("HOST")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USERNAME_DB")
PASSWORD = os.getenv("PASSWORD_DB")
logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler("chat_parser.log", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(
    logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
)
logger.addHandler(file_handler)


async def connect_to_database():
    while True:
        try:
            return await asyncpg.create_pool(
                host=HOST, database=DATABASE, user=USER, password=PASSWORD
            )
        except asyncpg.Error as e:
            logger.error(f"Error connecting to the database: {e}")
            logger.error("Reconnecting in 5 seconds...")
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
                placeholders = ", ".join(["$" + str(i + 1) for i in range(len(fields))])
                values = list(updates.values())
                if table_name == "user_chat":
                    select_query = f"SELECT * FROM {table_name} WHERE user_id = $1 AND chat_id = $2"
                    existing_record = await conn.fetchrow(
                        select_query, updates["user_id"], updates["chat_id"]
                    )
                else:
                    select_query = f"SELECT * FROM {table_name} WHERE {fields[0]} =$1 "
                    existing_record = await conn.fetchrow(
                        select_query, updates[fields[0]]
                    )
                if not existing_record:
                    insert_query = f"INSERT INTO {table_name} ({fields_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING "
                    await conn.execute(insert_query, *values)
                    logger.info(f"Добавлено в БД  {values}")
                elif table_name != "user_chat":
                    update_fields = [
                        f"{field} = ${i+1}" for i, field in enumerate(fields[0:])
                    ]
                    update_query = f"UPDATE {table_name} SET {', '.join(update_fields)} WHERE {fields[0]} = $1"
                    await conn.execute(update_query, *values)
                    logger.info(f"Обновление в БД  {values}")
                else:
                    logger.info(
                        f"Запись уже существует в БД: {values}. Обновление не требуется."
                    )

    except Exception as e:
        raise e


async def Users(data, pool):
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for key in data["accounts"]:
                    accounts_info = data["accounts"][key]["info"]
                    if (
                            accounts_info.get("username") is not None
                            and accounts_info.get("first_name") is not None
                    ):
                        username = accounts_info.get("username").lower()
                        last_online = (
                            datetime.datetime.strptime(
                                accounts_info.get("last_online"),
                                "%Y-%m-%d %H:%M:%S",
                            )
                            if accounts_info.get("last_online") is not None
                            else None
                        )
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

                        logger.info(
                            f"Инициализирую запрос на вставку в БД {update}"
                        )
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
                    for key_chat in data["accounts"][key]["chats"]:
                                update_user_chat = {
                                    "user_id": key,
                                    "chat_id": key_chat,
                                }
                                logger.info(
                                    f"Инициализирую запрос на вставку в БД {update_user_chat}"
                                )
                                await retry(
                                    insert_or_update_one,
                                    pool,
                                    "user_chat",
                                    ["user_id", "chat_id"],
                                    update_user_chat,
                                )

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error(sys.exc_info())


async def Chats(data, pool):
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for key in data["chats"]:
                    chats_key = data["chats"][key]
                    last_online = (
                        datetime.datetime.strptime(
                            chats_key.get("last_online"), "%Y-%m-%d %H:%M:%S"
                        )
                        if chats_key.get("last_online") is not None
                        else None
                    )
                    update = {
                        "chat_id": key,
                        "parent_link": chats_key.get("parent_link"),
                        "children_link": chats_key.get("children_link"),
                        "title": chats_key.get("title"),
                        "last_online": last_online,
                    }
                    logger.info(f"Инициализирую запрос на вставку в БД {update}")
                    await retry(
                        insert_or_update_one,
                        pool,
                        "chats",
                        [
                            "chat_id",
                            "parent_link",
                            "children_link",
                            "title",
                            "last_online",
                        ],
                        update,
                    )
    except Exception as e:
        logger.error(f"Error: {e}")


async def background_save(data):
    try:
        pool = await connect_to_database()
        await Chats(data, pool)
        await Users(data, pool)
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error(sys.exc_info())
    finally:
        await pool.close()
