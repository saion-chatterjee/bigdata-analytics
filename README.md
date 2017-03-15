# bigdata-analytics

This repository contains code for bigData Analytics using Kafka and Spark streaming.
(Flink to come)

Follow the below steps to clone code and setup your machine.


## Prerequisites

* Java8
* Maven


## 2. Getting code

           git clone https://github.com/saion-chatterjee/bigdata-analytics.git


## 3. Build

        mvn clean; 
        mvn install;


## 4 . Loading into an IDE

You can run all the examples from terminal. If you want to run from the IDE, follow the below steps


* ECLIPSE NEON.2

scp or git pull latest jar file before executing on cluster Driver machine.

## 5. Deployment

The code is configured to be deployed in a yarn managed cluster with spark-submit. 

Sample submission script:
./spark-submit --class com.saion.KafkaSparkAnalytics --master yarn --deploy-mode client /tmp/kafka-spark-0.0.1-SNAPSHOT.jar 'kafkaBrokerIP':6667 MyKafkaTest 100 0.01 0.00001 0 0.0001 5


Please read code comments for semantics of the parameters


Please ensure to start a Kafka Publisher and Consumer before executing this program else it will fail in runtime.

