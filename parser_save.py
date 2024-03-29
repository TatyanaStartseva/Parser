import sys
import sqlite3
from dotenv import load_dotenv

load_dotenv()
DB = "database.db"


async def Users(data):
    try:
        connection = sqlite3.connect(DB)
        cursor = connection.cursor()
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS Users (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        user_id INTEGER NOT NULL,
                                        username TEXT NOT NULL,
                                        bio TEXT, 
                                        first_name TEXT NOT NULL,
                                        last_name TEXT ,
                                        last_online DATETIME,
                                        premium BOOL,
                                        phone TEXT,
                                        image BOOl
                                        )
                                        """
        )
        for key in data["accounts"]:
            accounts_info = data["accounts"][key]["info"]
            username = None
            if accounts_info.get("username") is not None:
                username = accounts_info.get("username").lower()
            cursor.execute("SELECT id FROM Users WHERE user_id=?", (key,))
            existing_user = cursor.fetchone()
            if not existing_user:
                cursor.execute(
                    "INSERT INTO Users (user_id, username, bio, first_name, last_name, last_online, premium, phone, image) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        key,
                        username,
                        accounts_info.get("bio"),
                        accounts_info.get("first_name"),
                        accounts_info.get("last_name"),
                        accounts_info.get("last_online"),
                        accounts_info.get("premium"),
                        accounts_info.get("phone"),
                        accounts_info.get("image"),
                    ),
                )
            else:
                cursor.execute(
                    "UPDATE Users SET username=?, bio=?, first_name=?, last_name=?, last_online=?, premium=?, phone=?, image=? WHERE user_id =? ",
                    (
                        username,
                        accounts_info.get("bio"),
                        accounts_info.get("first_name"),
                        accounts_info.get("last_name"),
                        accounts_info.get("last_online"),
                        accounts_info.get("premium"),
                        accounts_info.get("phone"),
                        accounts_info.get("image"),
                        key,
                    ),
                )
        connection.commit()
        connection.close()
    except Exception as e:
        print(f"Error: {e}")
        print(sys.exc_info())


async def Messages(user_data):
    try:
        connection = sqlite3.connect(DB)
        cursor = connection.cursor()
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS Messages
                                    (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    message_id INTEGER NOT NULL,
                                    message TEXT,
                                    user_id INTEGER,
                                    chat_id INTEGER,
                                    FOREIGN KEY(message_id) REFERENCES Users(user_id),
                                    FOREIGN KEY(message_id) REFERENCES Chats(chat_id)
                                    )"""
        )
        for key in user_data["accounts"]:
            for key_chat in user_data["chats"]:
                for messages in user_data["accounts"][key]["chats"]:
                    for message_data in user_data["accounts"][key]["chats"][messages]:
                        message_id = message_data["message_id"]
                        text = message_data["text"]
                        cursor.execute(
                            "SELECT id FROM Messages WHERE message_id=?", (message_id,)
                        )
                        existing_messages = cursor.fetchone()
                        if not existing_messages:
                            cursor.execute(
                                "INSERT OR REPLACE INTO Messages (message_id, message,user_id,chat_id) VALUES (?,?,?,?)",
                                (message_id, text, key, key_chat),
                            )
        connection.commit()
        connection.close()
    except Exception as e:
        print(f"Error: {e}")
        print(sys.exc_info())


async def Chats(data):
    try:
        connection = sqlite3.connect(DB)
        cursor = connection.cursor()
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS Chats
                    (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL ,
                    username TEXT ,
                    title TEXT NOT NULL,
                    last_online DATETIME,
                    FOREIGN KEY(chat_id) REFERENCES Users(user_id))"""
        )

        for key in data["chats"]:
            chats_key = data["chats"][key]
            cursor.execute("SELECT id FROM Chats WHERE chat_id=?", (key,))
            existing_chat = cursor.fetchone()
            if existing_chat:
                cursor.execute(
                    """UPDATE Chats 
                    SET username=?, title=?, last_online=?
                    WHERE chat_id=?""",
                    (
                        chats_key.get("username"),
                        chats_key.get("title"),
                        chats_key.get("last_online"),
                        key,
                    ),
                )
            else:
                cursor.execute(
                    "INSERT INTO Chats (chat_id, username, title, last_online) VALUES (?, ?, ?, ?)",
                    (
                        key,
                        chats_key.get("username"),
                        chats_key.get("title"),
                        chats_key.get("last_online"),
                    ),
                )
        connection.commit()
        connection.close()
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
