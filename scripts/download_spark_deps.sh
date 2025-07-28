#!/bin/sh

JAR_FOLDER=$1
# keep hadoop version same as spark distribution
# iceberg currently works with java11
HADOOP_VERSION=3.3.4
ICEBERG_VERSION=1.6.1
cd $JAR_FOLDER
echo "Downloading Spark dependencies..."

wget -nc https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.787/aws-java-sdk-bundle-1.12.787.jar
wget -nc https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar
wget -nc https://repo1.maven.org/maven2/io/github/jaihind213/spark-set-udaf/spark3.5.2-scala2.13-1.0.1-jdk11/spark-set-udaf-spark3.5.2-scala2.13-1.0.1-jdk11-jar-with-dependencies.jar
wget -nc https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.13-${ICEBERG_VERSION}.jar
cd ..

