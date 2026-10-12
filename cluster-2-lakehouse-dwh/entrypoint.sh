#!/bin/bash
set -e

# 1. Khởi động OpenSSH daemon (phục vụ Airflow SSHOperator và Remote Execution)
if [ -x /usr/sbin/sshd ]; then
    mkdir -p /var/run/sshd
    /usr/sbin/sshd
    echo ">> [SSH] OpenSSH Daemon started successfully on port 22."
fi

# 2. Nếu là Spark Master node, khởi động thêm HiveThriftServer2 (cổng 10000) cho dbt-spark
if [ "$SPARK_MODE" = "master" ]; then
    (
        sleep 5
        su - spark -c "/opt/bitnami/spark/sbin/start-thriftserver.sh \
          --master 'local[*]' \
          --driver-memory 768m \
          --conf spark.driver.extraJavaOptions=--add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
          --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
          --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
          --conf spark.hadoop.fs.s3a.endpoint=http://minio-lakehouse:9000 \
          --conf spark.hadoop.fs.s3a.access.key=minioadmin \
          --conf spark.hadoop.fs.s3a.secret.key=minioadminpassword \
          --conf spark.hadoop.fs.s3a.path.style.access=true \
          --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
          --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" || true
        echo ">> [ThriftServer] Spark HiveThriftServer2 with Delta Lake & S3 started on port 10000."
    ) &
fi

# 3. Chạy Bitnami entrypoint và run command gốc
exec /opt/bitnami/scripts/spark/entrypoint.sh "$@"
