# Log Analyzer: real-time log pattern recognition and analysis platform
This is an application which analyzes the debugging logs from large scale distributed systems and automatically recognizes patterns that frequently appear in the log.
 
 # Introduction
Today large scale production systems are generating large amount of logs, E.g. Rakuten Viki reports logging 25k entries per second. It will be very useful if we can mine important information out of these log data so that developers or system administrators monitor system status or debug any issues. Existing log analysis system such as ELK can do pattern matching but they do not support recognizing new patterns. Therefore we designed a platform which can automatically recognize real-time patterns for log analysis.

# Architecture
The raw data is stored in S3, and ingested into kafka. These kafka messages are consumed by 2 flink pipelines.  
Pattern Recognizer pipeline batches input stream periodically and run pattern recognition algorithm to detect new patterns. The results are stored in timescaledb.  
Pattern matcher pipeline takes those patterns, match them with the real-time kafka log messages, and store the matched patterns and their occurrences in timescaledb.  
Finally data are visualized using grafana.  
![Architecture](https://user-images.githubusercontent.com/68665596/95793926-8d003f80-0c9b-11eb-9d17-02ecfc7fd7be.png)


# Datasets
77GB in total, 300+ million log entries from various systems.  
[Log Hub](https://github.com/logpai/loghub) 

# Engineer Challenge:

### Coordinate two pipelines
One engineer challenge is that we have to coordinate two pipelines. Recognizer pipeline periodically feeds new patterns to the matcher pipeline, and matcher pipeline needs to perform a join-stream operation which joins a high frequency, small size log stream with a low frequency, large-size pattern stream.  
Our solution is that we let the two pipeline talk to each other through a database. The patterns are written to a database first, and the matcher pipeline periodically loads those patterns from the database. and caches them in each flink operator. Then flink can perform efficient string matching using these cached patterns.

### Pattern Recognition Algorithm
Another challenge is about the recognition algorithm. We want to pick patterns that are both frequent and long. Short patterns such as INFO is frequent but too short to be informative.  
To solve this issue, we ranked the frequent patterns by combining the occurrences and length of the pattern into one score function: `score(pattern) = occurrences * pow(2,num_words)`
and we pick top-k highest ranked patterns. The rationale is that if you add one more word to the pattern, the score would increase if the expanded one’s frequency is at least 50% of its prefix, and decreases otherwise.    
Also, to implement a parallel algorithm, we used a breadth first search algorithm. It start from single-word tokens, find the frequent ones, and then expand them by recursively appending words to each of them. This naturally fits the map-reduce model of Flink framework.

### Performance Improvement

We tested and tuned Flink method/parameters to improve the performance of pattern matcher pipeline.
One effective way is to batch the database insert requests. However, if batch size is too big, our update will have high latency. We did The experiment to find the optimal batch size to both meet our throughput needs and minimize the latency. I run an experiments over 150 sec to record number of records inserted into database using different batch size. With batch size 10 and 100, we can catch up the speed that we generate matched patterns. While with batch size 1 and 5, we are lagging behind.  

![performance1](https://user-images.githubusercontent.com/68665596/95795641-b3c07500-0c9f-11eb-8b18-561199ed7d1e.png)
Another performace investigation is to improve the database query speed for my UI. I solved this by pre-aggregating per-second data and writing into a different table for visualization. By doing the aggregation the query speed is 1000 times faster.  

![performance2](https://user-images.githubusercontent.com/68665596/95795566-8247a980-0c9f-11eb-91e4-bc9dad306259.png)

# How to install and run
This project has 3 components:  
* Data Ingestion
* Data Processing
* Data Visualization

Data ingestion is implemented using Python. Raw data are stored in AWS S3. use below command to run:  
`log-data-ingestion/data_ingestion.py  ../config.ini log-file-name`  

Data Processing has 2 components: Matcher Job and Recognizer Job. Use Maven to build the project and generate JAR for Flink.  
Project configuration and dependencies are specified in `log-pattern-visualization\pom.xml`.  
After building you can setup Flink on AWS instances and run the following commands:
`~/flink-1.11.2/bin/flink run -yd -m [flink-ip:port] -p 6 -c xyz.dataprocess.matcher.MatcherJob target/logpattern-0.0.1-SNAPSHOT-jar-with-dependencies.jar`  
`~/flink-1.11.2/bin/flink run -yd -m [flink-ip:port] -c xyz.dataprocess.recognizer.RecognizerJob target/logpattern-0.0.1-SNAPSHOT-jar-with-dependencies.jar`  
Then you can start both matcher job and recognizer job. 

Data Visualization is a real-time visualizaiton on matched patterns and their occurrences. this component is also built by Maven. 
After building you can run this commond: `java -cp loganalyzer/log-pattern-visualization/target/visualization-0.0.1-SNAPSHOT-jar-with-dependencies.jar xyz.dataprocess.visualization.ConsoleVisualization`
