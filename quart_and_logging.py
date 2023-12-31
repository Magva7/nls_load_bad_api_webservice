import logging
from quart import Quart
import asyncio
import json
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
import os
import aiohttp

app = Quart(__name__)

# Установка секретного ключа Flask
app.secret_key = os.getenv('secret_key_for_app')

# logging.basicConfig(level=logging.DEBUG)  # Установка уровня логирования на DEBUG (временно)
logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Настройки БД - url, логин-пасс
username = os.getenv('username_for_sql')
password = os.getenv('password_for_sql')
DATABASE_URL = f"postgresql://{username}:{password}@localhost/mydatabase"

# Создаем объект для работы с БД
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

table_name = 'mydata'

# Определяем модели данных
class MyData(Base):
    __tablename__ = table_name

    id = Column(Integer, primary_key=True)
    primary_key_field = Column(String)
    # Тут потом, если надо, добавим еще поля

# базовый маршрут для проверки, что все работает - 127.0.0.1:5001
@app.route('/')
async def hello_world():
    logging.info("Hello, World!")  # Логируем сообщение
    return 'Hello, World!'

# Маршрут для синхронизации данных - http://127.0.0.1:5001/sync_data
@app.route('/sync_data', methods=['GET'])
async def sync_data():
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
        await asyncio.sleep(1)
        return "Data synchronization completed"
    except aiohttp.ClientError as e:
        logging.error(f"Ошибка: {e}")
        return f"Error: {e}"
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: : {e}")
        return f"Error decoding JSON: {e}"

# Функция для запуска Quart и цикла событий asyncio
async def main():
    # Запускаем Quart приложение
    await app.run_task(host='0.0.0.0', port=5001)

if __name__ == '__main__':
    # loop = asyncio.get_event_loop()
    
    # это не обязательно, 2 строки ниже можно закомментировать и оставить только ту, что выше,
    # в текущей версии работает и так, но предыдущую строку я закомментил и переписал
    # т.к. на Python 3.10 строка, которая выше еще работает, а на 3.11 уже не будет, лучше сразу так написать, 
    # чтобы предупреждения о том, что нет активного цикла не было - DeprecationWarning: There is no current event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(main())
