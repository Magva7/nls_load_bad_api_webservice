import asyncio
from quart import Quart
import json
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
import schedule
import os
import aiohttp
import logging

app = Quart(__name__)

# Настройки БД - url, логин-пасс
username = os.getenv('username_for_sql')
password = os.getenv('password_for_sql')
DATABASE_URL = f"postgresql://{username}:{password}@localhost/mydatabase"

# Создаем объект для работы с БД
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Определяем модели данных
class MyData(Base):
    __tablename__ = 'mydata'

    id = Column(Integer, primary_key=True)
    primary_key_field = Column(String)
    # Тут потом, если надо, добавим еще поля

# Асинхронная функция для получения данных
async def async_get_data():
    await asyncio.sleep(1)
    return 'Done!'

# Роут для синхронизации данных
@app.route('/sync_data', methods=['GET'])
async def sync_data():
    '''Функция синхронизации sync_data выполняется асинхронно'''
    api_url = 'https://google.com/api/data'

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as response:
                response.raise_for_status()
                data_from_api = await response.json()

        # Получаем данные из БД
        session = Session()
        try:
            existing_data = session.query(MyData).all()

            # Создаем набор primary key значений для сравнения
            existing_primary_keys = set(obj.primary_key_field for obj in existing_data)
            api_primary_keys = set(obj['primary_key_field'] for obj in data_from_api)

            # Ищем объекты, которые есть в JSON, но нет в базе БД, и создаем их
            new_objects = [MyData(primary_key_field=obj['primary_key_field']) for obj in data_from_api if obj['primary_key_field'] not in existing_primary_keys]
            session.add_all(new_objects)

            # Обновляем строки данных там, где что-то поменялось
            for obj in data_from_api:
                if obj['primary_key_field'] in existing_primary_keys:
                    existing_obj = session.query(MyData).filter_by(primary_key_field=obj['primary_key_field']).first()
                    for key, value in obj.items():
                        if key != 'primary_key_field' and getattr(existing_obj, key) != value:
                            setattr(existing_obj, key, value)

            # Помечаем на удаление записи, которые есть в БД, но их нет в json
            for obj in existing_data:
                if obj.primary_key_field not in api_primary_keys:
                    session.delete(obj)

            session.commit()
        except Exception as e:
            session.rollback()
            logging.error(f"Ошибка: {e}")
            raise e
        finally:
            session.close()

        logging.info("Data synchronization completed")
        return "Data synchronization completed"
    except aiohttp.ClientError as e:
        return f"Error: {e}"
    except json.JSONDecodeError as e:
        return f"Error decoding JSON: {e}"

# Запускаем цикл для выполнения задач в фоновом режиме
async def schedule_loop():
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            logging.error(f"Ошибка в schedule_loop: {e}")
        await asyncio.sleep(1)


@app.route("/data")
async def get_data():
    data = await async_get_data()
    return data

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(schedule_loop())

    # Запуск Quart сервера
    try:
        app.run(host='0.0.0.0', port=5001, use_reloader=False)  # 5001 порт для отладки
    except Exception as e:
        logging.error(f"Ошибка при запуске Quart: {e}")

    # Настройки логирования после запуска Quart
    # logging.basicConfig(filename='app.log', level=logging.INFO)
    logging.basicConfig(filename=r'D:\nls_load_bad_api_webservice\app.log', level=logging.INFO, filemode='w')
    logging.info('Quart запущен успешно')
