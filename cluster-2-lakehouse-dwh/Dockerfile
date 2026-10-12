FROM bitnamilegacy/spark:3.5.0

USER root

# 1. Cài đặt các công cụ hệ thống & OpenSSH Server
RUN apt-get update && apt-get install -y --no-install-recommends openssh-server procps sudo && \
    rm -rf /var/lib/apt/lists/*

# Cấu hình SSH Server và tạo user spark (UID 1001)
RUN useradd -m -u 1001 -s /bin/bash spark 2>/dev/null || true && \
    echo "spark:spark" | chpasswd && \
    usermod -aG root spark && \
    mkdir -p /var/run/sshd && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    echo "PermitUserEnvironment yes" >> /etc/ssh/sshd_config

# Thiết lập biến môi trường cho SSH session
RUN cat << 'EOF_PROFILE' > /etc/profile.d/spark.sh
export SPARK_HOME=/opt/bitnami/spark
export JAVA_HOME=/opt/bitnami/java
export PATH=/opt/bitnami/python/bin:/opt/bitnami/java/bin:/opt/bitnami/spark/bin:/opt/bitnami/spark/sbin:$PATH
export PYTHONPATH=/opt/bitnami/spark/python/:$PYTHONPATH
EOF_PROFILE
RUN cat << 'EOF_ENV' >> /etc/environment
SPARK_HOME=/opt/bitnami/spark
JAVA_HOME=/opt/bitnami/java
PATH=/opt/bitnami/python/bin:/opt/bitnami/java/bin:/opt/bitnami/spark/bin:/opt/bitnami/spark/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
PYTHONPATH=/opt/bitnami/spark/python/
EOF_ENV
RUN cat << 'EOF_BASHRC' >> /home/spark/.bashrc
export SPARK_HOME=/opt/bitnami/spark
export JAVA_HOME=/opt/bitnami/java
export PATH=/opt/bitnami/python/bin:/opt/bitnami/java/bin:/opt/bitnami/spark/bin:/opt/bitnami/spark/sbin:$PATH
export PYTHONPATH=/opt/bitnami/spark/python/:$PYTHONPATH
EOF_BASHRC
RUN chown -R spark:spark /home/spark

# 2. Cài đặt các thư viện Python cho Spark Runner
RUN pip install --no-cache-dir pyspark==3.5.0 delta-spark==3.1.0 oracledb>=2.0.0

# 3. Tải sẵn đầy đủ các file JARs cốt lõi (Delta Lake, S3 MinIO, Kafka Streaming & Oracle JDBC)
ADD https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.9.0.0/ojdbc8-21.9.0.0.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/bitnami/spark/jars/

# 4. Phân quyền truy cập các file JAR
RUN chmod 644 /opt/bitnami/spark/jars/*.jar

# 5. Entrypoint wrapper script để tự động chạy SSHD
COPY entrypoint.sh /opt/bitnami/scripts/spark/custom_entrypoint.sh
RUN chmod +x /opt/bitnami/scripts/spark/custom_entrypoint.sh

ENTRYPOINT [ "/opt/bitnami/scripts/spark/custom_entrypoint.sh" ]
CMD [ "/opt/bitnami/scripts/spark/run.sh" ]
