import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import time
import os

log_file = '/logs/metric_log.csv'
output_image = '/logs/error_distribution.png'

os.makedirs('/logs', exist_ok=True)
while True:
    try:
        if os.path.exists(log_file):
            data = pd.read_csv(log_file)

            if not data.empty:
                plt.figure(figsize=(8, 5))
                sns.histplot(data['absolute_error'], kde=True, color='orange', bins=10)
                plt.title('Distribution of Absolute Errors')
                plt.xlabel('absolute_error')
                plt.ylabel('Count')

                plt.savefig(output_image)
                plt.close()
                print(f"Гистограмма обновлена: {output_image}")
            else:
                print("Файл метрик пуст, ожидаем новых данных...")
        else:
            print("Файл метрик не найден, ожидаем его появления...")

    except Exception as e:
        print(f"Ошибка при построении графика: {e}")

    time.sleep(10)