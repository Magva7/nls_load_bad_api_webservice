from flask import Flask
import requests
import json
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import schedule
import time
from threading import Thread

app = Flask(__name__)

# Настройки БД - url, логин-пасс
DATABASE_URL = "postgresql://username:password@localhost/mydatabase"

# Создаем объект для работы с БД
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Определяем модели данных
class MyData(Base):
    __tablename__ = 'mydata'

    id = Column(Integer, primary_key=True)
    primary_key_field = Column(String)
    # Тут потом если надо добавим еще поля

# Роут для синхронизации данных
@app.route('/sync_data', methods=['GET'])
def sync_data():
    api_url = 'https://example.com/api/data'

    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data_from_api = response.json()

        # Получаем данные из БД
        session = Session()
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
        session.close()

        return "Data synchronization completed"
    except requests.exceptions.RequestException as e:
        return f"Error: {e}"
    except json.JSONDecodeError as e:
        return f"Error decoding JSON: {e}"

# функция для запуска функции синхронизации    
def run_sync_task():
    sync_data()

# запускаем функция для запуска функции синхронизации раз в минуту
def schedule_loop():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    # Создаем поток для выполнения цикла с расписанием
    schedule_thread = Thread(target=schedule_loop)
    schedule_thread.start()

    # Запуск Flask сервера
    # app.run(debug=True)
    app.run(host='0.0.0.0', port=5001)  # 5001 порт для отладки
