# landing_to_bronze.py
import requests
from pyspark.sql import SparkSession
import os
import sys

def download_data(table_name, directory):
    """Завантаження файлу з FTP сервера"""
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + table_name + ".csv"
    print(f"Завантажуємо з {downloading_url}")
    response = requests.get(downloading_url)

    if response.status_code == 200:
        os.makedirs(directory, exist_ok=True)
        csv_file_path = os.path.join(directory, f"{table_name}.csv")
        
        with open(csv_file_path, "wb") as file:
            file.write(response.content)
        print(f"Файл успішно завантажено та збережено як {csv_file_path}")
    else:
        raise Exception(f"Помилка завантаження файлу. Код статусу: {response.status_code}")

def landing_to_bronze_fn(shared_dir):
    """Основна функція для перетворення Landing в Bronze"""
    spark = SparkSession.builder.appName("Landing to Bronze").getOrCreate()

    for table in ["athlete_bio", "athlete_event_results"]:
        
        # Завантажити файл з ftp-сервера в оригінальному форматі csv
        download_data(table, directory=shared_dir)

        # За допомогою Spark прочитати csv-файл
        csv_file = os.path.join(shared_dir, f"{table}.csv")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_file)

        bronze_dir = os.path.join(shared_dir, "bronze", table)
        os.makedirs(bronze_dir, exist_ok=True)
        
        # Зберегти його у форматі parquet у папку bronze/{table}
        df.write.mode("overwrite").parquet(bronze_dir)

        print(f"Таблиця {table} успішно перетворена в Parquet у {bronze_dir}")
        print(f"Кількість записів: {df.count()}")
        print("Приклад даних:")
        df.show(10)
        
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        shared_dir = sys.argv[1]
    else:
        # Резервний шлях для локального тестування
        shared_dir = "/Users/margosha/Documents/GoIt/goit-de-fp/part_2/datalake"
    
    landing_to_bronze_fn(shared_dir)