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
import logging
import pyzipper

load_dotenv()

api_id = os.getenv("TELEGRAM_API_ID")
api_hash = os.getenv("TELEGRAM_API_HASH")
channel_id = -1001935880746 # channel that aggregates all the logs - has to stay the same for the bot to work, as its set up for their specific format - its static though so you dont have to worry about that
MAX_CONCURRENT_DOWNLOADS = 1 # change this to however many files you want to download at once if multiple are dropped at the same time, above 3 is where telegram starts to get pissy about it
download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
download_queue = asyncio.Queue()
process_queue = asyncio.Queue()

def setup_logger(log_file='app.log', log_level=logging.INFO):
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

logger = setup_logger()

async def download_worker():
    while True:
        event, file_path, password = await download_queue.get()
        try:
            async with download_semaphore:
                file_path = await event.message.download_media(file=file_path)
                logger.info(f'New file downloaded to {file_path}')
                await process_queue.put((file_path, password))
        except Exception as e:
            logger.error(f"Error downloading file: {e}")
        finally:
            download_queue.task_done()

async def process_worker():
    while True:
        file_path, password = await process_queue.get()
        try:
            extract_path = os.path.splitext(file_path)[0]
            os.makedirs(extract_path, exist_ok=True)
            extract_file(file_path, extract_path, password)
            os.remove(file_path)
            shutil.rmtree(extract_path)
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
        finally:
            process_queue.task_done()

def line_count(file_path):
    with open(file_path, 'r') as file:
        line_count = sum(1 for line in file if line.strip() and not line.lower().startswith('android://'))
    return line_count


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
        logger.error(f"Failed to send Discord webhook. Status code: {response.status_code}")

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
    
    logger.info(f"Number of valid lines in {file_path}: {line_count}")

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
                        logger.warning(f"Skipping invalid line: {line}")
                except Exception as e:
                    logger.error(f"Error processing line: {line}. Error: {e}")
    
    completion_message = f"Data insertion completed. Inserted {inserted_count} documents out of {line_count} valid lines in {file_path}."
    logger.info(completion_message)

    send_discord_webhook(webhook_url, completion_message)

def extract_file(file_path, extract_path, password=None):
    file_extension = os.path.splitext(file_path)[1].lower()
    errors = []

    try:
        if file_extension == '.zip':
            extract_zip(file_path, extract_path, password)
        elif file_extension == '.rar':
            extract_rar_with_unrar(file_path, extract_path, password)
        else:
            logger.error(f"Unsupported file format: {file_extension}")
            return
        logger.info(f"Successfully extracted {file_path} to {extract_path}")
        recursive_extract(extract_path, None)
        
        shutil.copy2('rg.deb', extract_path)
        
        rg_path = os.path.join(extract_path, 'rg.deb')
        os.chmod(rg_path, 0o755)

        try:
            cmd1 = 'rg -oUNI "URL:\\s(.*?)[|\\r]\\nUsername:\\s(.*?)[|\\r]\\nPassword:\\s(.*?)[|\\r]\\n" -r \'$1:$2:$3\' --glob-case-insensitive -g "Passwords.txt" | uniq >> combined.txt'
            subprocess.run(cmd1, shell=True, cwd=extract_path, check=True)
            logger.info(f"Executed 1st rg command in {extract_path}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error executing 1st rg command: {e}")
            errors.append(f"1st rg command failed: {e}")
        
        try:
            cmd2 = 'rg -oUNI "URL:\s(.*)\nUSER:\s(.*)\nPASS:\s(.*)" -r \'$1:$2:$3\' --multiline --glob-case-insensitive -g "All Passwords.txt" | tr -d \'\r\' | uniq >> combined.txt'
            subprocess.run(cmd2, shell=True, cwd=extract_path, check=True)
            logger.info(f"Executed 2nd rg command in {extract_path}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error executing 2nd rg command: {e}")
            errors.append(f"2nd rg command failed: {e}")
        
        subprocess.run('sort combined.txt | uniq -u > unique.txt', shell=True, cwd=extract_path, check=True, stdout=subprocess.DEVNULL)

        if len(errors) < 2:
            logger.info(f"Proceeding with data ingestion with {len(errors)} error(s).")
            logger.info(f"Amount of lines before dedupe: {line_count(os.path.join(extract_path, 'combined.txt'))}")
            logger.info(f"Amount of lines: {line_count(os.path.join(extract_path, 'unique.txt'))}")
            if line_count(os.path.join(extract_path, 'unique.txt')) > 0:
                ingest_data(os.path.join(extract_path, 'unique.txt'), os.getenv("DISCORD_WEBHOOK"))
        else:
            logger.error("Too many errors, skipping data ingestion.")
            logger.error("\n".join(errors))

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

def extract_zip(file_path, extract_path, password=None):
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            if password:
                zip_ref.setpassword(password.encode())
            _extract_all(zip_ref, extract_path)
    except (RuntimeError, zipfile.BadZipFile, NotImplementedError):
        try:
            with pyzipper.AESZipFile(file_path, 'r') as zip_ref:
                if password:
                    zip_ref.pwd = password.encode()
                _extract_all(zip_ref, extract_path)
        except (RuntimeError, zipfile.BadZipFile, NotImplementedError) as e:
            logger.error(f"Failed to extract zip file: {e}")
            raise

def _extract_all(zip_ref, extract_path):
    for member in zip_ref.namelist():
        try:
            zip_ref.extract(member, extract_path)
        except OSError as e:
            if e.errno == errno.ENAMETOOLONG:
                logger.warning(f"Skipping file in archive due to path length: {member}")
            else:
                raise

def extract_rar_with_unrar(file_path, extract_path, password=None):
    try:
        os.makedirs(extract_path, exist_ok=True)
        cmd = ["unrar", "x", "-o+", file_path, extract_path]
        if password:
            cmd.insert(2, f"-p{password}")
        
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info(f"Successfully extracted {file_path} to {extract_path}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error extracting {file_path}: {e.stderr.decode()}")

def recursive_extract(path, password=None):
    for root, dirs, files in os.walk(path):
        for file in files:
            try:
                file_path = os.path.join(root, file)
                file_extension = os.path.splitext(file)[1].lower()
                if file_extension in ['.zip', '.rar'] and os.path.getsize(file_path) > 100 * 1024 * 1024:  # 100MB
                    logger.info(f"Found large archive: {file_path}")
                    new_extract_path = os.path.splitext(file_path)[0]
                    try:
                        os.makedirs(new_extract_path, exist_ok=True)
                    except OSError as e:
                        if e.errno == errno.ENAMETOOLONG:
                            logger.warning(f"Skipping file due to path length: {file_path}")
                            continue
                        else:
                            raise
                    
                    if file_extension == '.zip':
                        extract_zip(file_path, new_extract_path, password)
                    elif file_extension == '.rar':
                        extract_rar_with_unrar(file_path, new_extract_path, password)
                    
                    logger.info(f"Successfully extracted {file_path} to {new_extract_path}")
                    os.remove(file_path)
                    recursive_extract(new_extract_path, password)
            except OSError as e:
                if e.errno == errno.ENAMETOOLONG:
                    logger.error(f"Skipping file due to path length: {os.path.join(root, file)}")
                else:
                    logger.error(f"Unexpected error processing file {os.path.join(root, file)}: {e}")

async def handler(event):
    if event.message.media:
        chat_id = event.chat_id
        message_id = event.message.id
        message_link = f'tg://privatepost?channel={chat_id}&post={message_id}'
        
        if event.message.file:
            original_filename = event.message.file.name or f'message{message_id}'
            file_size = event.message.file.size
        else:
            original_filename = f'message{message_id}'
            file_size = 0 
        
        valid_filename = original_filename.replace('/', '').replace('\\', '_')
        valid_filename = unicodedata.normalize('NFKD', valid_filename)
        
        try:
            valid_filename.encode('utf-8')
        except UnicodeEncodeError:
            valid_filename = f'message{message_id}'
        
        if len(valid_filename) > 255:
            valid_filename = valid_filename[:250] + f'{message_id}'
        
        message_text = event.message.message.strip() if event.message.message else ""

        password_match = re.search(r'.pass: (.*?)(?:\n|$)', message_text, re.DOTALL)
        password = password_match.group(1).strip() if password_match else None

        if password == '?':
            password = None
        
        logger.info(f'Message link: {message_link}')
        logger.info(f'Filename: {valid_filename}')
        logger.info(f'File size: {file_size / (1024 * 1024):.2f} MB')
        
        if password:
            logger.info(f'Password: {password}')
        else:
            logger.info('No password found or password is ignored.')
        
        file_path = f'./{valid_filename}'
        await download_queue.put((event, file_path, password))

async def main():
    await client.start()

    workers = [
        asyncio.create_task(download_worker()) for _ in range(MAX_CONCURRENT_DOWNLOADS)
    ] + [
        asyncio.create_task(process_worker()) for _ in range(MAX_CONCURRENT_DOWNLOADS)
    ]
    
    logger.info("Workers started, listening for messages...")

    @client.on(events.NewMessage(chats=channel_id))
    async def handler_wrapper(event):
        await handler(event)

    try:
        await client.run_until_disconnected()
    finally:
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

with client:
    client.loop.run_until_complete(main())
