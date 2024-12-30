import pika
import json
import os

# Путь к файлу для логирования метрик
log_file = os.path.join('/logs', 'metric_log.csv')

# Создаём папку logs, если её нет
os.makedirs('/logs', exist_ok=True)

# Инициализируем CSV-файл, если он не существует
if not os.path.exists(log_file):
    with open(log_file, 'w') as file:
        file.write('id,y_true,y_pred,absolute_error\n')

# Хранилище для сообщений
buffer = {}


def log_metrics(message_id, y_true=None, y_pred=None):
    """
    Логирует метрики в файл, если доступны y_true и y_pred.
    """
    # Сохраняем данные в буфер
    if message_id not in buffer:
        buffer[message_id] = {'y_true': None, 'y_pred': None}
    if y_true is not None:
        buffer[message_id]['y_true'] = y_true
    if y_pred is not None:
        buffer[message_id]['y_pred'] = y_pred

    if buffer[message_id]['y_true'] is not None and buffer[message_id]['y_pred'] is not None:
        y_true_value = buffer[message_id]['y_true']
        y_pred_value = buffer[message_id]['y_pred']

        # Вычисляем абсолютную ошибку
        try:
            absolute_error = abs(y_true_value - y_pred_value)
        except TypeError as e:
            print(f"Ошибка при вычислении абсолютной ошибки: {e}")
            return

        with open(log_file, 'a') as file:
            file.write(f"{message_id},{y_true_value},{y_pred_value},{absolute_error}\n")

        # Удаляем сообщение из буфера
        del buffer[message_id]


try:
    # Создаём подключение к RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    # Объявляем очередь y_true
    channel.queue_declare(queue='y_true')
    # Объявляем очередь y_pred
    channel.queue_declare(queue='y_pred')


    # Callback для обработки сообщений
    def callback_y_true(ch, method, properties, body):
        try:
            message = json.loads(body)
            print(f"Получено сообщение из y_true: {message}")
            log_metrics(message['id'], y_true=message['body'])
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Ошибка при обработке сообщения из y_true: {e}")


    def callback_y_pred(ch, method, properties, body):
        try:
            message = json.loads(body)
            print(f"Получено сообщение из y_pred: {message}")
            log_metrics(message['id'], y_pred=message['body'])
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Ошибка при обработке сообщения из y_pred: {e}")


    # Потребляем сообщения из y_true
    channel.basic_consume(
        queue='y_true',
        on_message_callback=callback_y_true,
        auto_ack=True
    )

    # Потребляем сообщения из y_pred
    channel.basic_consume(
        queue='y_pred',
        on_message_callback=callback_y_pred,
        auto_ack=True
    )

    # Запускаем режим ожидания прихода сообщений
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()

except Exception as e:
    print(f"Не удалось подключиться к очереди: {e}")