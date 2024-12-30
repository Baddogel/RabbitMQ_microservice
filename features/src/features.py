import pika
import numpy as np
import json
from sklearn.datasets import load_diabetes
from datetime import datetime
import time
 
# Загружаем датасет о диабете
X, y = load_diabetes(return_X_y=True)


# Создаём бесконечный цикл для отправки сообщений в очередь
while True:
    try:
        # Формируем случайный индекс строки
        random_row = np.random.randint(0, X.shape[0]-1)

        # Создаём уникальный идентификатор на основе timestamp
        message_id = datetime.timestamp(datetime.now())
 
        # Создаём подключение по адресу rabbitmq:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
 
        # Создаём очереди, если они не существуют
        channel.queue_declare(queue='y_true')
        channel.queue_declare(queue='features')
 
        # Формируем и отправляем сообщение в очередь y_true
        message_y_true = {
            'id': message_id,
            'body': y[random_row]
        }

        channel.basic_publish(exchange='',
                              routing_key='y_true',
                              body=json.dumps(message_y_true))
        print(f"Сообщение с правильным ответом отправлено в очередь: {message_y_true}")

        # Формируем и отправляем сообщение в очередь features
        message_features = {
            'id': message_id,
            'body': list(X[random_row])
        }
        
        channel.basic_publish(exchange='',
                              routing_key='features',
                              body=json.dumps(message_features))
        print(f"Сообщение с вектором признаков отправлено в очередь: {message_features}")
 
        # Закрываем подключение
        connection.close()

        # Задержка перед следующей итерацией
        time.sleep(10)
    except Exception as e:
        print(f"Не удалось подключиться к очереди: {e}")