# kafka_producer.py
from pyspark.sql import SparkSession
import json
from datetime import datetime

def send_test_message():
    print("Відправляємо тестове повідомлення в Kafka...")
    
    spark = SparkSession.builder \
        .appName("Kafka Producer Test") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
        .master("local[2]") \
        .getOrCreate()
    
    # Створюємо тестове повідомлення - результат змагань
    test_data = {
        "athlete_id": 65649,  # ID існуючого атлета з вашої бази
        "event": "Swimming 100m Freestyle",
        "medal": "Gold",
        "result": "47.05",
        "timestamp": datetime.now().isoformat()
    }
    
    # Перетворюємо в JSON
    json_message = json.dumps(test_data)
    print(f"Повідомлення: {json_message}")
    
    # Створюємо DataFrame для відправки
    message_df = spark.createDataFrame([
        (str(test_data["athlete_id"]), json_message)
    ], ["key", "value"])
    
    # Відправляємо в Kafka
    try:
        message_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
            .option("topic", "athlete_event_results") \
            .option("kafka.security.protocol", "SASL_PLAINTEXT") \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
            .save()
        
        print("Тестове повідомлення відправлено!")
        
    except Exception as e:
        print(f"Помилка відправки: {e}")
    
    spark.stop()

if __name__ == "__main__":
    send_test_message()