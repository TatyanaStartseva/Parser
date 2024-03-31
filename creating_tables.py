import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("HOST")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USERNAME_DB")
PASSWORD = os.getenv("PASSWORD_DB")

conn = psycopg2.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)
cursor = conn.cursor()

create_chats_table_query = """
CREATE TABLE IF NOT EXISTS Chats (
    id SERIAL PRIMARY KEY,
    chat_id BIGINT UNIQUE,
    parent_link TEXT,
    children_link TEXT,
    title TEXT,
    last_online TIMESTAMP
)
"""
cursor.execute(create_chats_table_query)
conn.commit()

create_users_table_query = """
CREATE TABLE IF NOT EXISTS Users (
    id SERIAL PRIMARY KEY,
    user_id BIGINT UNIQUE,
    username TEXT,
    bio TEXT,
    first_name TEXT,
    last_name TEXT,
    last_online TIMESTAMP,
    premium BOOL,
    phone TEXT,
    image BOOL,
    chat_id BIGINT,
    FOREIGN KEY (chat_id) REFERENCES Chats(chat_id) ON DELETE CASCADE
)
"""
cursor.execute(create_users_table_query)
conn.commit()
create_messages_table_query = """
CREATE TABLE IF NOT EXISTS Messages (
    id SERIAL PRIMARY KEY,
    message_id INTEGER UNIQUE,
    message TEXT,
    user_id BIGINT,
    chat_id BIGINT,
    FOREIGN KEY (user_id) REFERENCES Users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (chat_id) REFERENCES Chats(chat_id) ON DELETE CASCADE
)
"""
cursor.execute(create_messages_table_query)
conn.commit()