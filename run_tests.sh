#!/usr/bin/env bash

# TODO: set this variable to your top level of Spark 2.4.4 installation
SPARK_244_HOME=/path/to/your/spark-2.4.4-bin-hadoop2.7

if [ ! -d $SPARK_244_HOME ]
then
  echo "Invalid spark home directory: $SPARK_244_HOME"
  echo "Please edit this file and update SPARK_244_HOME"
  exit 1
fi

mvn clean package
$SPARK_244_HOME/bin/spark-submit --class edu.stanford.cs245.Tester target/cs245-as2-1.0-SNAPSHOT.jar
