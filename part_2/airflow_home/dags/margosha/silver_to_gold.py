# silver_to_gold.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col
import sys

def silver_to_gold_fn(shared_dir):
    """Основна функція для перетворення Silver в Gold"""
    if not os.path.exists(shared_dir):
        raise FileNotFoundError(f"Спільна директорія {shared_dir} не існує.")
    
    spark = SparkSession.builder.appName("Calculate Average Stats").getOrCreate()

    input_dir_bio = os.path.join(shared_dir, "silver", "athlete_bio")
    input_dir_event = os.path.join(shared_dir, "silver", "athlete_event_results")

    # Зчитувати дві таблиці: silver/athlete_bio та silver/athlete_event_results
    athlete_bio = spark.read.parquet(input_dir_bio)
    athlete_event = spark.read.parquet(input_dir_event)
    
    print("Схема athlete_bio:")
    athlete_bio.printSchema()
    print("Схема athlete_event:")
    athlete_event.printSchema()

    # Підготувати bio таблицю
    athlete_bio = athlete_bio \
        .select("athlete_id", "sex", "height", "weight", "country_noc") \
        .withColumn("height", col("height").cast("float")) \
        .withColumn("weight", col("weight").cast("float"))

    # Підготувати event таблицю
    athlete_event = athlete_event.select("athlete_id", "sport", "medal")

    # Робити join за колонкою athlete_id
    joined = athlete_bio.join(athlete_event, "athlete_id", "inner")
    
    print(f"Об'єднано записів: {joined.count()}")

    # Для кожної комбінації цих 4 стовпчиків — sport, medal, sex, country_noc — знаходити середні значення weight і height
    result = joined.groupBy("sport", "medal", "sex", "country_noc") \
                   .agg(
                       avg("height").alias("avg_height"),
                       avg("weight").alias("avg_weight")
                   )
    
    # Додати колонку timestamp з часовою міткою виконання програми
    result = result.withColumn("timestamp", current_timestamp())

    output_dir = os.path.join(shared_dir, "gold", "avg_stats")
    os.makedirs(output_dir, exist_ok=True)

    # Записувати дані в gold/avg_stats
    result.write.mode("overwrite").parquet(output_dir)

    print(f"Таблиця успішно агрегована та збережена у {output_dir}")
    print(f"Результуючих записів: {result.count()}")
    print("Приклад результатів:")
    result.show(20)
    
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        shared_dir = sys.argv[1]
    else:
        # Резервний шлях для локального тестування
        shared_dir = "/Users/margosha/Documents/GoIt/goit-de-fp/part_2/datalake"
    
    silver_to_gold_fn(shared_dir)