# bronze_to_silver.py
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
import sys

def bronze_to_silver_fn(shared_dir):
    """Основна функція для перетворення Bronze в Silver"""
    if not os.path.exists(shared_dir):
        raise FileNotFoundError(f"Спільна директорія {shared_dir} не існує.")
    
    spark = SparkSession.builder.appName("Bronze to Silver").getOrCreate()
    
    # Паттерн для очищення тексту
    pattern = r'[^a-zA-Z0-9,.\\"\'\s]'

    for table in ["athlete_bio", "athlete_event_results"]:
        
        # Зчитувати таблицю bronze
        input_dir = os.path.join(shared_dir, "bronze", table)
        df = spark.read.parquet(input_dir)
        print(f"Прочитано з {input_dir} успішно. Записів: {df.count()}")
       
        # Виконувати функцію чистки тексту для всіх текстових колонок
        for col_name, col_type in df.dtypes:
            if col_type == "string":
                df = df.withColumn(col_name, regexp_replace(col_name, pattern, ''))
        print("Дані очищено")

        # Робити дедуплікацію рядків
        original_count = df.count()
        df = df.dropDuplicates()
        final_count = df.count()
        print(f"Видалено дублікатів: {original_count - final_count} рядків")

        # Записувати таблицю в папку silver/{table}
        output_dir = os.path.join(shared_dir, "silver", table)
        os.makedirs(output_dir, exist_ok=True)

        df.write.mode("overwrite").parquet(output_dir)
        print(f"Таблиця {table} успішно очищена та збережена у {output_dir}")
        print(f"Фінальна кількість записів: {final_count}")
        print("Приклад даних:")
        df.show(10)
        
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        shared_dir = sys.argv[1]
    else:
        # Резервний шлях для локального тестування
        shared_dir = "/Users/margosha/Documents/GoIt/goit-de-fp/part_2/datalake"
    
    bronze_to_silver_fn(shared_dir)