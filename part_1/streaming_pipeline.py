# streaming_pipeline.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Конфігурація
# MySQL налаштування
MYSQL_URL = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
MYSQL_USER = "neo_data_admin"
MYSQL_PASSWORD = "Proyahaxuqithab9oplp"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"
MYSQL_TABLE = "athlete_bio"

# Kafka налаштування
KAFKA_BOOTSTRAP_SERVERS = "77.81.230.104:9092"
KAFKA_USER = "admin"
KAFKA_PASSWORD = "VawEzo1ikLtrA8Ug8THa"
KAFKA_INPUT_TOPIC = "athlete_event_results"
KAFKA_OUTPUT_TOPIC = "athlete_enriched_agg"

def create_spark_session():
    """Створення SparkSession з MySQL та Kafka коннекторами"""
    return SparkSession.builder \
        .appName("Olympic Data Streaming Pipeline") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
        .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .master("local[*]") \
        .getOrCreate()

# Етап 1: Зчитування даних атлетів з MySQL
def read_athlete_data(spark):
    """Читання даних атлетів з MySQL"""
    print("Читаємо дані атлетів з MySQL...")
    
    try:
        df = spark.read.format('jdbc').options(
            url=MYSQL_URL,
            driver=MYSQL_DRIVER,
            dbtable=MYSQL_TABLE,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        ).load()
        
        print(f"Завантажено {df.count()} записів з MySQL")
        print("Схема таблиці:")
        df.printSchema()
        return df
        
    except Exception as e:
        print(f"Помилка читання MySQL: {e}")
        raise

# Етап 2: Фільтрація даних де показники зросту та ваги не є порожніми
def filter_athlete_data(df):
    """Фільтрація даних атлетів"""
    print("Фільтруємо дані атлетів...")
    
    # Фільтрація по фізичним показникам
    filtered_df = df.filter(
        (col("weight").isNotNull()) & 
        (col("height").isNotNull()) &
        (col("sex").isNotNull()) &
        (col("country_noc").isNotNull())  # National Olympic Committee (країна)
    )
    
    print(f"Після фільтрації: {filtered_df.count()} записів")
    return filtered_df

# Етап 3: Зчитування даних з результатами змагань з Kafka-топіку
def read_kafka_stream(spark):
    """Читання потоку результатів змагань з Kafka"""
    print("Підключаємося до Kafka стріму...")
    
    # Kafka налаштування з аутентифікацією
    kafka_options = {
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "subscribe": KAFKA_INPUT_TOPIC,
        "startingOffsets": "latest",
        "kafka.security.protocol": "SASL_PLAINTEXT",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";'
    }
    
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()
    
    # Парсинг JSON повідомлень з Kafka
    # Схема повідомлень
    schema = StructType([
        StructField("athlete_id", IntegerType(), True),
        StructField("event", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("result", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("message_key"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("message_key", "data.*", "kafka_timestamp")
    
    return parsed_df

# Етап 4: Об'єднання даних з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці
def enrich_streaming_data(streaming_df, athlete_df):
    """Об'єднання потокових даних з даними атлетів"""
    print("Об'єднуємо потокові дані з даними атлетів...")
    
    # Створюємо alias для DataFrame'ів для уникнення неоднозначності
    streaming_aliased = streaming_df.alias("stream")
    athlete_aliased = athlete_df.alias("athlete")
    
    # Stream-to-static join з використанням alias
    enriched_df = streaming_aliased.join(
        athlete_aliased, 
        col("stream.athlete_id") == col("athlete.athlete_id"),
        "left"
    ).select(
        # Вибираємо колонки з потоку
        col("stream.message_key"),
        col("stream.athlete_id"),
        col("stream.event"),
        col("stream.medal"), 
        col("stream.result"),
        col("stream.timestamp").alias("event_timestamp"),
        col("stream.kafka_timestamp"),
        # Вибираємо колонки зі статичних даних атлетів
        col("athlete.name"),
        col("athlete.sex"),
        col("athlete.born"),
        col("athlete.height"),
        col("athlete.weight"),
        col("athlete.country"),
        col("athlete.country_noc")
    )
    
    # Додаємо трансформації для ML ознак
    enriched_df = enriched_df.withColumn(
        "bmi", 
        when(col("height").isNotNull() & col("weight").isNotNull(),
             col("weight") / (pow(col("height") / 100, 2))
        ).otherwise(None)
    ).withColumn(
        "has_medal",
        when(col("medal").isNotNull() & (col("medal") != "NA"), 1).otherwise(0)
    ).withColumn(
        "processing_time",
        current_timestamp()
    )
    
    return enriched_df

# Етап 5: Знаходження середнього зросту і ваги атлетів індивідуально для кожного виду спорту, типу медалі, статі, країни
def foreach_batch_function(batch_df, batch_id):
    """Функція обробки кожного мікробатчу"""
    print(f"\nОбробляємо батч {batch_id}")
    print(f"Кількість записів в батчі: {batch_df.count()}")
    
    if batch_df.count() > 0:
        try:
            print("Схема вхідних даних:")
            batch_df.printSchema()
            
            print("Сирі дані з Kafka:")
            batch_df.select("message_key", "athlete_id", "event", "medal").show(5, truncate=False)
            
            # Групування та розрахунок середніх значень
            aggregated_df = batch_df.groupBy("event", "medal", "sex", "country_noc") \
                .agg(
                    avg("height").alias("avg_height"),
                    avg("weight").alias("avg_weight"),
                    current_timestamp().alias("timestamp")
                )
            
            print("Розраховані середні значення:")
            aggregated_df.show(5, truncate=False)
            
            # Етап 6а: Стрім даних у вихідний кафка-топік
            print(f"Відправляємо дані в Kafka топік: {KAFKA_OUTPUT_TOPIC}")
            
            kafka_output_df = aggregated_df.select(
                concat_ws("_", col("event"), col("medal"), col("sex"), col("country_noc")).alias("key"),
                to_json(struct(
                    col("event").alias("sport"),
                    col("medal"),
                    col("sex"),
                    col("country_noc"),
                    col("avg_height"),
                    col("avg_weight"),
                    col("timestamp")
                )).alias("value")
            )
            
            kafka_options = {
                "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "topic": KAFKA_OUTPUT_TOPIC,
                "kafka.security.protocol": "SASL_PLAINTEXT",
                "kafka.sasl.mechanism": "PLAIN",
                "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";'
            }
            
            kafka_output_df.write \
                .format("kafka") \
                .options(**kafka_options) \
                .save()
            
            print("Дані відправлені в Kafka")
            
            # Етап 6б: Стрім даних у базу даних
            print("Зберігаємо в MySQL...")
            
            # Вибираємо необхідні колонки для збереження
            save_df = aggregated_df.select(
                col("event").alias("sport"),
                col("medal"),
                col("sex"),
                col("country_noc"),
                col("avg_height"),
                col("avg_weight"),
                col("timestamp")
            )
            
            print("Дані для збереження в MySQL:")
            save_df.show(5, truncate=False)
            
            save_df.write \
                .format("jdbc") \
                .option("url", MYSQL_URL) \
                .option("driver", MYSQL_DRIVER) \
                .option("dbtable", "athlete_enriched_agg") \
                .option("user", MYSQL_USER) \
                .option("password", MYSQL_PASSWORD) \
                .mode("append") \
                .save()
            
            print("Дані збережені в MySQL")
            
            print(f"Батч {batch_id} успішно оброблено: {batch_df.count()} записів")
            
        except Exception as e:
            print(f"Помилка в батчі {batch_id}: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"Батч {batch_id} порожній - очікуємо нові дані...")
    
    print(f"Кінець обробки батчу {batch_id}\n")

def main():
    """Основна функція"""
    print("Запускаємо потоковий пайплайн Olympic Data...")
    print("=" * 60)
    
    # Створення Spark сесії
    spark = create_spark_session()
    
    try:
        # Етап 1: Читання та фільтрація даних атлетів з MySQL
        print("\nЕтап 1: Підготовка статичних даних")
        athlete_df = read_athlete_data(spark)
        filtered_athlete_df = filter_athlete_data(athlete_df)
        
        # Кешування статичних даних для оптимізації
        filtered_athlete_df.cache()
        
        print("Приклади даних атлетів:")
        filtered_athlete_df.select("athlete_id", "name", "sex", "born", "height", "weight", "country_noc").show(5)
        
        # Етап 2: Налаштування потоку з Kafka
        print("\nЕтап 2: Налаштування Kafka стріму")
        streaming_df = read_kafka_stream(spark)
        
        # Етап 3: Збагачення потокових даних
        print("\nЕтап 3: Налаштування збагачення даних")
        enriched_stream = enrich_streaming_data(streaming_df, filtered_athlete_df)
        
        # Етап 4: Запуск стріму з forEachBatch
        print("\nЕтап 4: Запуск потокового пайплайна")
        query = enriched_stream.writeStream \
            .foreachBatch(foreach_batch_function) \
            .outputMode("append") \
            .trigger(processingTime="30 seconds") \
            .option("checkpointLocation", "./checkpoint") \
            .start()
        
        print("Стрім успішно запущено!")
        print(f"Spark UI: http://localhost:4040")
        print(f"Читаємо з Kafka топіка: {KAFKA_INPUT_TOPIC}")
        print(f"Відправляємо в Kafka топік: {KAFKA_OUTPUT_TOPIC}")
        print(f"Зберігаємо в MySQL таблицю: athlete_enriched_agg")
        print("Для зупинки натисніть Ctrl+C")
        print("=" * 60)
        
        # Очікування завершення
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nОтримано сигнал зупинки...")
    except Exception as e:
        print(f"Критична помилка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("Очищуємо ресурси...")
        spark.stop()
        print("Пайплайн зупинено")

if __name__ == "__main__":
    main()