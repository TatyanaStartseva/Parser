import sys
import psycopg2
import os
import time
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("HOST")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USERNAME_DB")
PASSWORD = os.getenv("PASSWORD_DB")


def connect_to_database():
    while True:
        try:
            conn = psycopg2.connect(
                host=HOST, database=DATABASE, user=USER, password=PASSWORD
            )
            cursor = conn.cursor()
            return conn, cursor
        except psycopg2.Error as e:
            print(f"Error connecting to the database: {e}")
            print("Reconnecting in 5 seconds...")
            time.sleep(5)


def retry(func_name, *args, **kwargs):
    def _wrapper():
        while True:
            try:
                func_name(*args, **kwargs)
                break
            except Exception as e:
                print(f"Произошла ошибка: {e}. Повторная попытка...")
                time.sleep(1)

    _wrapper()


def insert_or_update_one(cursor, conn, table_name, fields, updates):
    try:
        fields_str = ", ".join(fields)
        placeholders = ", ".join(["%(" + field + ")s" for field in fields])
        select_query = f"SELECT * FROM {table_name} WHERE {fields[0]} = %(key_value)s"
        cursor.execute(select_query, {"key_value": updates[fields[0]]})
        existing_record = cursor.fetchone()
        if existing_record is not None:
            if table_name == "user_chat" or table_name == "messages":
                if (
                    existing_record[fields.index("user_id")] != updates["user_id"]
                    or existing_record[fields.index("chat_id")] != updates["chat_id"]
                ):
                    insert_query = f"INSERT INTO {table_name} ({fields_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
                    cursor.execute(insert_query, updates)
            else:
                update_str = ", ".join(
                    [f"{field} = %({field})s" for field in updates.keys()]
                )
                update_query = f"UPDATE {table_name} SET {update_str} WHERE {fields[0]} = %(key_value)s"
                cursor.execute(
                    update_query, {"key_value": updates[fields[0]], **updates}
                )
        else:
            insert_query = f"INSERT INTO {table_name} ({fields_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
            cursor.execute(insert_query, updates)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def Users(data, cursor, conn):
    try:
        for key in data["accounts"]:
            accounts_info = data["accounts"][key]["info"]
            chat_id = list(data["accounts"][key]["chats"].keys())[0]
            if (
                accounts_info.get("username") is not None
                and accounts_info.get("first_name") is not None
            ):
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
                update_user_chat = {
                    "user_id": key,
                    "chat_id": chat_id,
                }
                retry(
                    insert_or_update_one,
                    cursor,
                    conn,
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
                retry(
                    insert_or_update_one,
                    cursor,
                    conn,
                    "user_chat",
                    ["user_id", "chat_id"],
                    update_user_chat,
                )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        print(sys.exc_info())


def Chats(data, cursor, conn):
    try:
        for key in data["chats"]:
            chats_key = data["chats"][key]
            update = {
                "chat_id": key,
                "parent_link": chats_key.get("parent_link"),
                "children_link": chats_key.get("children_link"),
                "title": chats_key.get("title"),
                "last_online": chats_key.get("last_online"),
            }
            retry(
                insert_or_update_one,
                cursor,
                conn,
                "chats",
                ["chat_id", "parent_link", "children_link", "title", "last_online"],
                update,
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")


def Messages(user_data, cursor, conn):
    try:
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
                        retry(
                            insert_or_update_one,
                            cursor,
                            conn,
                            "messages",
                            ["message_id", "message", "user_id", "chat_id"],
                            update,
                        )
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()


def background_save(data):
    try:
        conn, cursor = connect_to_database()
        cursor = conn.cursor()
        Chats(data, cursor, conn)
        Users(data, cursor, conn)
        Messages(data, cursor, conn)
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        print(sys.exc_info())
    finally:
        cursor.close()
        conn.close()
