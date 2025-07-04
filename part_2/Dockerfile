# Dockerfile
FROM apache/airflow:2.10.3

# Переключаемся на root для установки системных пакетов
USER root

# Устанавливаем Java и обновляем пакеты
RUN apt-get update && \
    apt-get install -y default-jdk wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Настраиваем переменные окружения для Java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Скачиваем и устанавливаем Spark
ENV SPARK_VERSION=3.5.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Добавляем Spark в PATH
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Создаем необходимые директории с правильными правами (пока мы root)
RUN mkdir -p /opt/airflow/dags /opt/shared && \
    chown -R airflow:root /opt/shared && \
    chmod -R 755 /opt/shared

# Переключаемся обратно на airflow пользователя
USER airflow

# Устанавливаем Python пакеты
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.9.0 \
    pyspark==3.5.2 \
    requests

# Устанавливаем рабочую директорию
WORKDIR /opt/airflow

# Команда по умолчанию
CMD ["airflow", "standalone"]