#!/bin/bash

# Script to download required JAR files for Iceberg, Nessie, and S3 support

JARS_DIR="./jars"
mkdir -p $JARS_DIR

# Iceberg Spark Runtime
echo "Downloading Iceberg Spark Runtime..."
wget -O $JARS_DIR/iceberg-spark-runtime-3.5_2.13-1.9.2.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.13/1.9.2/iceberg-spark-runtime-3.5_2.13-1.9.2.jar
  # https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.13/1.4.3/iceberg-spark-runtime-3.5_2.13-1.4.3.jar

# Iceberg Nessie
echo "Downloading Iceberg Nessie..."
wget -O $JARS_DIR/iceberg-nessie-1.9.2.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-nessie/1.9.2/iceberg-nessie-1.9.2.jar

# # Nessie Iceberg
# echo "Downloading Nessie Iceberg..."
# wget -O $JARS_DIR/nessie-iceberg-1.0.0.jar \
#   https://repo1.maven.org/maven2/org/projectnessie/nessie-iceberg/1.0.0/nessie-iceberg-1.0.0.jar


# AWS S3 Support
echo "Downloading AWS Java SDK Bundle..."
wget -O $JARS_DIR/aws-java-sdk-bundle-1.12.565.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.565/aws-java-sdk-bundle-1.12.565.jar

# Hadoop AWS
echo "Downloading Hadoop AWS..."
wget -O $JARS_DIR/hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

echo "All JAR files downloaded successfully!"
echo "Files in $JARS_DIR:"
ls -la $JARS_DIR
