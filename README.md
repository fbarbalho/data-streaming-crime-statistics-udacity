# Udacity SF Crime Statistics with Spark Streaming


#### Steps to Execute:

1. Run Server:
    * `/usr/bin/zookeeper-server-start zookeeper.properties`
    * `/usr/bin/kafka-server-start server.properties`

2. Run Producer and Kafka Server:
    * `python producer_server.py`
    * `python kafka_server.py`
    
3. Run kafka console consumer to check the process:
    * kafka-console-consumer --topic police.department.calls.event --bootstrap-server localhost:9092  --from-beginning

4. Run Streaming Spark Application (data_stream.py):
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py`

#### Questions:

1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

    SparkSession provide us some important properties that affect throughput and latency of the data in our application.
    
    We can use this properties to improve the speed that the application processes new rows using parameters like:

    * inputRowsPerSecond  
    * processedRowsPerSecond 
    

2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

    It is important to say that there is no perfect configuration. We need to monitor our application and adjust certain parameters as necessary to have the best performance.
    
    We can increase performance and data ingestion using the correct parameters like:
    
    * spark.streaming.kafka.maxRatePerPartition
    * spark.default.parallelism
    
    In my tests the best configuration at the moment for me was
    
    * spark.streaming.kafka.maxRatePerPartition: 3
    * spark.default.parallelism: 10