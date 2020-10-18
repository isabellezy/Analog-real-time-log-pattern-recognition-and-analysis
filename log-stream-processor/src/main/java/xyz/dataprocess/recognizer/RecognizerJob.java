package xyz.dataprocess.recognizer;

import java.util.*;
import java.time.*;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import java.util.Properties;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import java.util.Properties;
import java.io.FileInputStream;

public class RecognizerJob {
    static Logger LOG = LoggerFactory.getLogger(RecognizerJob.class);
    private static int patternMinOccurrence;
    private static int topKnum;
    private static final OutputTag<Tuple2<String, Integer>> freqPatternTag = new OutputTag<Tuple2<String, Integer>>("side-output") {};
    //private static final String s3SinkPath = "s3a://ka-app-<username>/data";

    public static void main(String[] args) throws Exception {
         //LOG.error("Setup finished");
 
        Properties prop=new Properties();
        FileInputStream ip= new FileInputStream("config.properties");
        prop.load(ip);
        
        String kafkaAddress = prop.getProperty("kafkaAddress");
        String inputTopic = prop.getProperty("inputTopic");
        String consumerGroup = prop.getProperty("recognizerconsumerGroup");
        patternMinOccurrence = Integer.parseInt(prop.getProperty("patternMinOccurrence"));
        //LOG.info("Min Occurrence: "+ PATTERN_MIN_OCCURRENCE);
        topKnum = Integer.parseInt(prop.getProperty("topKnum"));
        //LOG.info("Top K num" + topKnum);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaAddress);
        properties.setProperty("group.id", consumerGroup);
        
        FlinkKafkaConsumer<ObjectNode> flinkKafkaConsumer = new FlinkKafkaConsumer<>(inputTopic, new JSONKeyValueDeserializationSchema(false), properties);
        flinkKafkaConsumer.assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)));
        //FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>(outputTopic, new SimpleStringSchema(), properties);
        
        DataStream<ObjectNode> stream = env.addSource(flinkKafkaConsumer);
        
        DataStream<String> logStream = stream.map(new MapFunction<ObjectNode, String>() {
            @Override
            public String map(ObjectNode objectNode) {
                String log = objectNode.get("value").get("log").asText();
                // LOG.info("Object received. Timestamp: {}", objectNode.get("value").get("timestamp"));
                return log;
            }
        });
        
        DataStream<Tuple2<String, String>> patternStream = logStream.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
		    public void flatMap(String log, Collector<Tuple2<String, String>> out) {
    			//String[] tokens = log.split(" ");
    			int start = 0;
    			int index = 0;
    			while (index < log.length()) {
    			    while (index < log.length() && log.charAt(index) == ' ') {
    			        index++;
    			    }
    			    start = index;
    			    if (start == log.length()) {
    			        return;
    			    }
    			    while (index < log.length() && log.charAt(index) != ' ') {
    			        index++;
    			    }
    			    String key = log.substring(start, index);
    			    String logSuffix = log.substring(start);
    			    
    			    out.collect(new Tuple2<String, String>(key, logSuffix));
    			}
		    }
        });
        
        DataStream<Tuple2<String, Integer>> combined = null;
        int patternlen = 1;
        while ( patternlen <= 10 ) {
            SingleOutputStreamOperator<Tuple2<String, String>> outputStream  = patternStream
                .keyBy(value -> value.f0)
                .timeWindow(Time.seconds(60))
                .process(new FindAndExpandPatterns());
            DataStream<Tuple2<String, Integer>> sideStream = outputStream.getSideOutput(freqPatternTag);
            //sideStream.timeWindow(Time.seconds(15))
                
            //sideStream.print();
            if (combined == null) {
                combined = sideStream;
            } else {
                combined = combined.union(sideStream);
            } 
            
            patternStream = outputStream;
            patternlen++;
  
        }
        //combined.print();
        DataStream<Row> rankedRow = combined.timeWindowAll(Time.seconds(60)).apply(new AllWindowFunction<Tuple2<String,Integer>, Row, TimeWindow>() {
            private int topKnum = RecognizerJob.topKnum;
            public void apply (TimeWindow window,
                    Iterable<Tuple2<String, Integer>> values,
                    Collector<Row> out) throws Exception {
                ArrayList<Tuple3<String, Integer, Double>> valuelist = new ArrayList<>();
                for (Tuple2<String, Integer> v : values) {
                    int numTokens = v.f0.split(" +").length;
                    valuelist.add(new Tuple3<String, Integer, Double>(v.f0, v.f1, v.f1 * Math.pow(2, numTokens)));
                }
                ////////
                System.out.printf("rank function called. Entries: %d\n", valuelist.size());
                
                Collections.sort(valuelist, (a, b) -> {
                     return b.f2.compareTo(a.f2);
                });
                
                int min = Math.min(valuelist.size(), topKnum);
                ////////
               // System.out.printf("topKnum: %d min: %d\n", topKnum, min);
              // return top 20 patterns
                for(int i = 0; i < min; i++) {
                    Row row = new Row(4); 
                   // row.setField(0, new Timestamp((long)(aTuple.f0.doubleValue() * 1000))); 
                    row.setField(0, new Timestamp(window.getEnd()));
                    row.setField(1, valuelist.get(i).f0);
                    row.setField(2, valuelist.get(i).f1);
                    row.setField(3, valuelist.get(i).f2);
                    out.collect(row);
                    
                }
                
            }
        });
        
        String query = "INSERT INTO rankedpattern (time, pattern, occurrence, score) VALUES (?, ?, ?, ?);";
        JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
            .setDrivername(prop.getProperty("drivername"))
            .setDBUrl(prop.getProperty("dburl"))
            .setUsername(prop.getProperty("dbusername"))
            .setPassword(prop.getProperty("dbpassword"))
            .setQuery(query)
            .setSqlTypes(new int[] { Types.TIMESTAMP, Types.VARCHAR, Types.INTEGER, Types.DOUBLE})
            .setBatchInterval(0)
            .finish();
        assert(jdbcOutput != null);
        
        rankedRow.writeUsingOutputFormat(jdbcOutput);

        //rankedStream.addSink(new createS3Sink());
    
        env.execute("");
    }
    
    static class FindAndExpandPatterns extends ProcessWindowFunction<
        Tuple2<String, String>, Tuple2<String, String>, String, TimeWindow> {
        int patternMinOccurrence = RecognizerJob.patternMinOccurrence;
        @Override
        public void process(String pattern,
                            Context ctx,
                            Iterable<Tuple2<String, String>> values,
                            Collector<Tuple2<String, String>> out) throws Exception {
            // LOG.info("New window received, pattern {}", pattern);
            List<Tuple2<String, String>> stagedValues = new ArrayList<>();
            values.forEach(stagedValues::add);
            if (stagedValues.size() < patternMinOccurrence) {
                return;
            }
            //LOG.info("pattern: {}, frequency: {}", pattern, stagedValues.size());
            ctx.output(freqPatternTag, new Tuple2(pattern, stagedValues.size()));

            for (Tuple2<String, String> v : stagedValues) {
                String entry = v.f1;
                String expanded = expandPattern(pattern, entry);
                if (expanded != null) {
                    out.collect(new Tuple2(expanded, entry));
                }
            }
        }
     
    }

    private static String expandPattern(String pattern, String entry) {
        int index =  pattern.length();
        while (index < entry.length() && entry.charAt(index) == ' ') {
          index++;
        }
        if (index >= entry.length()) {
            return null;
        }
        while (index < entry.length() && entry.charAt(index) != ' ') {
            index++;
        }
        return entry.substring(0, index);
    }
    
}