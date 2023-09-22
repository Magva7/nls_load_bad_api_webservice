import logging
from quart import Quart
import asyncio
import json
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
import schedule
import os
import aiohttp

app = Quart(__name__)

# Настройка логирования
logging.basicConfig(filename='app.log', level=logging.INFO)  # Здесь указываем имя файла для логов

# базовый маршрут для проверки, что все работает - 127.0.0.1:5001
@app.route('/')
async def hello_world():
    logging.info("Hello, World!")  # Логируем сообщение
    return 'Hello, World!'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
