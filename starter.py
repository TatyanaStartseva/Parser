import subprocess
import json

# Чтение данных из файла sessions.json
with open("sessions.json", "r") as file:
    sessions = json.load(file)

# Проход по каждому объекту и вызов команды в консоли
for session in sessions:
    api_id = session["api_id"]
    api_hash = session["api_hash"]
    session_value = session["session_value"]

    # Формирование строки для вызова команды
    command = f"pm2 start parser.py --interpreter=python3 --name {api_id} -- {api_id} {api_hash} {session_value}"

    # Вызов команды в консоли
    subprocess.run(command, shell=True)
