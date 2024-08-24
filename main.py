from telethon import TelegramClient, events
import os
import re
import zipfile
import shutil
import subprocess
import pymongo
from datetime import datetime as dt
import requests
import asyncio
import errno
from dotenv import load_dotenv
import unicodedata

load_dotenv()

api_id = os.getenv("TELEGRAM_API_ID")
api_hash = os.getenv("TELEGRAM_API_HASH")
channel_id = -1001935880746 # channel that aggregates all the logs - has to stay the same for the bot to work, as its set up for their specific format - its static though so you dont have to worry about that

client = TelegramClient('Session', api_id, api_hash)


username = os.getenv("MONGO_USERNAME")
password = os.getenv("MONGO_PASSWORD")
host = os.getenv("MONGO_HOST")
port = os.getenv("MONGO_PORT")
database = os.getenv("MONGO_DATABASE")
collection = os.getenv("MONGO_COLLECTION")

mongo_uri = f"mongodb://{username}:{password}@{host}:{port}"

mongo_client = pymongo.MongoClient(mongo_uri)
db = mongo_client[database]
collection = db[collection]

def send_discord_webhook(webhook_url, message):
    data = {
        "content": message
    }
    response = requests.post(webhook_url, json=data)
    if response.status_code != 204:
        print(f"Failed to send Discord webhook. Status code: {response.status_code}")

def parse_line(line):
    parts = line.strip().split(':')
    if len(parts) < 3:
        return None

    if parts[0] in ['http', 'https']:
        url = f"{parts[0]}:{parts[1]}"
        email = parts[2]
        password = ':'.join(parts[3:])
    else:
        url = parts[0]
        email = parts[1]
        password = ':'.join(parts[2:])

    current_date = dt.now().strftime("%d_%m_%Y")
    source_db = f"stealer_logs_{current_date}"
    
    return {
        "email": email,
        "password": password,
        "url": url,
        "source_db": source_db
    }
    
def ingest_data(file_path, webhook_url):
    with open(file_path, 'r') as file:
        line_count = sum(1 for line in file if line.strip() and not line.lower().startswith('android://'))
    
    print(f"Number of valid lines in {file_path}: {line_count}")

    inserted_count = 0
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if line and not line.lower().startswith('android://'):
                try:
                    document = parse_line(line)
                    if document:
                        collection.insert_one(document)
                        inserted_count += 1
                    else:
                        print(f"Skipping invalid line: {line}")
                except Exception as e:
                    print(f"Error processing line: {line}. Error: {e}")
    
    completion_message = f"Data insertion completed. Inserted {inserted_count} documents out of {line_count} valid lines in {file_path}."
    print(completion_message)

    send_discord_webhook(webhook_url, completion_message)

def extract_file(file_path, extract_path, password=None):
    file_extension = os.path.splitext(file_path)[1].lower()
    try:
        if file_extension == '.zip':
            extract_zip(file_path, extract_path, password)
        elif file_extension == '.rar':
            extract_rar_with_unrar(file_path, extract_path, password)
        else:
            print(f"Unsupported file format: {file_extension}")
            return
        print(f"Successfully extracted {file_path} to {extract_path}")
        recursive_extract(extract_path, None)
        
        shutil.copy2('rg.deb', extract_path)
        
        rg_path = os.path.join(extract_path, 'rg.deb')
        os.chmod(rg_path, 0o755)
        
        cmd = 'rg -oUNI "URL:\\s(.*?)[|\\r]\\nUsername:\\s(.*?)[|\\r]\\nPassword:\\s(.*?)[|\\r]\\n" -r \'$1:$2:$3\' --glob-case-insensitive -g "Passwords.txt" > combined.txt'
        subprocess.run(cmd, shell=True, cwd=extract_path, check=True)
        
        print(f"Executed rg command in {extract_path}")
        
        ingest_data(os.path.join(extract_path, 'combined.txt'), os.getnev('DISCORD_WEBHOOK_URL'))
    
    except zipfile.BadZipFile:
        print(f"Error: {file_path} is not a valid ZIP file.")
    except subprocess.CalledProcessError as e:
        print(f"Error executing rg command: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def extract_zip(file_path, extract_path, password=None):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        if password:
            zip_ref.setpassword(password.encode())
        for member in zip_ref.namelist():
            try:
                zip_ref.extract(member, extract_path)
            except OSError as e:
                if e.errno == errno.ENAMETOOLONG:
                    print(f"Skipping file in archive due to path length: {member}")
                else:
                    raise

def extract_rar_with_unrar(file_path, extract_path, password=None):
    try:
        os.makedirs(extract_path, exist_ok=True)
        cmd = ["unrar", "x", "-o+", file_path, extract_path]
        if password:
            cmd.insert(2, f"-p{password}")
        
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"Successfully extracted {file_path} to {extract_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error extracting {file_path}: {e.stderr.decode()}")

def recursive_extract(path, password=None):
    for root, dirs, files in os.walk(path):
        for file in files:
            try:
                file_path = os.path.join(root, file)
                file_extension = os.path.splitext(file)[1].lower()
                if file_extension in ['.zip', '.rar'] and os.path.getsize(file_path) > 100 * 1024 * 1024:  # 100MB
                    print(f"Found large archive: {file_path}")
                    new_extract_path = os.path.splitext(file_path)[0]
                    try:
                        os.makedirs(new_extract_path, exist_ok=True)
                    except OSError as e:
                        if e.errno == errno.ENAMETOOLONG:
                            print(f"Skipping file due to path length: {file_path}")
                            continue
                        else:
                            raise
                    
                    if file_extension == '.zip':
                        extract_zip(file_path, new_extract_path, password)
                    elif file_extension == '.rar':
                        extract_rar_with_unrar(file_path, new_extract_path, password)
                    
                    print(f"Successfully extracted {file_path} to {new_extract_path}")
                    os.remove(file_path)
                    recursive_extract(new_extract_path, password)
            except OSError as e:
                if e.errno == errno.ENAMETOOLONG:
                    print(f"Skipping file due to path length: {os.path.join(root, file)}")
                else:
                    print(f"Unexpected error processing file {os.path.join(root, file)}: {e}")

download_queue = asyncio.Queue()

async def process_queue():
    while True:
        event, file_path, password = await download_queue.get()
        try:
            extract_path = os.path.splitext(file_path)[0]
            os.makedirs(extract_path, exist_ok=True)
            extract_file(file_path, extract_path, password)
            os.remove(file_path)
            shutil.rmtree(extract_path)
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
        finally:
            download_queue.task_done()

async def handler(event):
    if event.message.media:
        chat_id = event.chat_id
        message_id = event.message.id

        message_link = f'tg://privatepost?channel={chat_id}&post={message_id}'

        if event.message.file:
            original_filename = event.message.file.name or f'message_{message_id}'
        else:
            original_filename = f'message_{message_id}'

        valid_filename = original_filename.replace('/', '_').replace('\\', '_')
        valid_filename = unicodedata.normalize('NFKD', valid_filename)

        try:
            valid_filename.encode('utf-8')
        except UnicodeEncodeError:
            valid_filename = f'message_{message_id}'

        if len(valid_filename) > 255:
            valid_filename = valid_filename[:250] + f'_{message_id}'

        message_text = event.message.message
        print(message_text)
        
        password_match = re.search(r'.pass: (.*?)(?:\n|$)', message_text, re.DOTALL)
        password = password_match.group(1).strip() if password_match else None

        if password == '➖':
            password = None

        print(f'Message link: {message_link}')
        print(f'Filename: {valid_filename}')
        if password:
            print(f'Password: {password}')
        else:
            print('No password found or password is ignored.')

        file_path = await event.message.download_media(file=f'./{valid_filename}')
        print(f'New file downloaded to {file_path}')

        await download_queue.put((event, file_path, password))

async def main():
    await client.start()

    asyncio.create_task(process_queue())

    @client.on(events.NewMessage(chats=channel_id))
    async def handler_wrapper(event):
        await handler(event)

    await client.run_until_disconnected()

with client:
    client.loop.run_until_complete(main())
