# kafka_test.py
from pyspark.sql import SparkSession

def test_spark_352():
    print("Тестуємо PySpark 3.5.2 + Kafka...")
    
    spark = SparkSession.builder \
        .appName("PySpark 3.5.2 Test") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
        .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
        .config("spark.sql.adaptive.enabled", "false") \
        .master("local[2]") \
        .getOrCreate()
    
    print(f"Spark версія: {spark.version}")
    
    try:
        # Тест MySQL
        mysql_df = spark.read.format('jdbc').options(
            url="jdbc:mysql://217.61.57.46:3306/olympic_dataset",
            driver="com.mysql.cj.jdbc.Driver",
            dbtable="athlete_bio",
            user="neo_data_admin",
            password="Proyahaxuqithab9oplp"
        ).load()
        
        print(f"MySQL: {mysql_df.count()} записів")
        
        # Тест Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
            .option("subscribe", "athlete_event_results") \
            .option("startingOffsets", "latest") \
            .load()
        
        print("Kafka працює!")
        kafka_df.printSchema()
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"Помилка: {e}")
        spark.stop()
        return False

if __name__ == "__main__":
    test_spark_352()