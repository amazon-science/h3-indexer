#!/bin/bash

set -e

# setup h3-indexer-env directory
rm -rf $HOME/h3-indexer-env
mkdir $HOME/h3-indexer-env
cd $HOME/h3-indexer-env

export H3=$HOME/h3-indexer-env

# get spark and move into proper directory
cd $H3
mkdir $H3/spark-3.3.0-amzn-1-bin-3.3.3-amzn-0
wget https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-4.0/spark-3.3.0-amzn-1-bin-3.3.3-amzn-0.tgz
tar xzvf spark-3.3.0-amzn-1-bin-3.3.3-amzn-0.tgz -C $H3/spark-3.3.0-amzn-1-bin-3.3.3-amzn-0

# install aws-glue-libs
cd $H3
git clone https://github.com/awslabs/aws-glue-libs.git

# install maven
cd $H3
curl https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-common/apache-maven-3.6.0-bin.tar.gz --output apache-maven-3.6.0-bin.tar.gz
tar xvzf apache-maven-3.6.0-bin.tar.gz
export M2_HOME=$H3/apache-maven-3.6.0
export PATH=$M2_HOME/bin:$PATH

# install Java
if command -v yum >/dev/null 2>&1; then
  sudo yum install -y java-1.8.0-openjdk
  JAVAPATH=$(ls -d /usr/lib/jvm/java-1.8.0-openjdk-1.8.0*)
elif command -v apt-get >/dev/null 2>&1; then
  sudo apt-get update
  sudo apt-get install -y openjdk-8-jdk
  JAVAPATH=$(ls -d /usr/lib/jvm/java-8-openjdk-* | head -n 1)
else
  echo "Error installing Java 8."
  exit 1
fi

# setup aws-glue-libs
cd $H3/aws-glue-libs
./bin/gluepyspark || true
rm $H3/aws-glue-libs/jarsv1/*log4j*

# install Athena JDBC
cd $H3/aws-glue-libs/jarsv1/
wget https://downloads.athena.us-east-1.amazonaws.com/drivers/JDBC/SimbaAthenaJDBC-2.2.1.1000/AthenaJDBC42-2.2.1.1000.jar

# export all environment variables
export SPARK_HOME=$H3/spark-3.3.0-amzn-1-bin-3.3.3-amzn-0/spark
export GLUE_JARS=$H3/aws-glue-libs/jarsv1/*
export JAVA_HOME=$JAVAPATH/jre

echo "Your SPARK_HOME path is $H3/spark-3.3.0-amzn-1-bin-3.3.3-amzn-0/spark"
echo "Your GLUE_JARS path is $H3/aws-glue-libs/jarsv1/*"
echo "Your JAVA_HOME path is $JAVAPATH/jre"