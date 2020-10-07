package xyz.dataprocess.matcher;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import java.util.Properties;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import java.sql.Timestamp;
import java.sql.Types;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import java.util.Properties;
import java.io.FileInputStream;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.time.Duration;

public class MatcherJob {
    static Logger LOG = LoggerFactory.getLogger(MatcherJob.class);
    
    public static void main(String[] args) throws Exception {
         //LOG.error("Setup finished");
 
        Properties prop=new Properties();
        FileInputStream ip= new FileInputStream("config.properties");
        prop.load(ip);
        
        String kafkaAddress = prop.getProperty("kafkaAddress");
        String inputTopic = prop.getProperty("inputTopic");
        String consumerGroup = prop.getProperty("matcherconsumerGroup");
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaAddress);
        properties.setProperty("group.id", consumerGroup);
        
        FlinkKafkaConsumer<ObjectNode> flinkKafkaConsumer = new FlinkKafkaConsumer<>(inputTopic, new JSONKeyValueDeserializationSchema(false), properties); 
        //FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>(outputTopic, new SimpleStringSchema(), properties);
        flinkKafkaConsumer.assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)));
        
        DataStream<ObjectNode> stream = env.addSource(flinkKafkaConsumer);
        
        DataStream<LogEntry> logStream = stream.map(new MapFunction<ObjectNode, LogEntry>() {
            @Override
            public LogEntry map(ObjectNode objectNode) {
                String log = objectNode.get("value").get("log").asText();
                double timestamp = objectNode.get("value").get("timestamp").asDouble();
                LogEntry logEntry = new LogEntry(timestamp, log);
                return logEntry;
            }
        });
        
        
  
        //logStream.flatMap(new PatternMatcher()).keyBy(value -> value.f0).timeWindow(Time.milliseconds(100)).sum(1).print();//.addSink(flinkKafkaProducer);
        DataStream<Tuple3<Double, String, Integer>> tuplestream = logStream.flatMap(new PatternMatcher(prop));
        String query = "INSERT INTO pattern (time, pattern, occurrence) VALUES (?, ?, ?);";
        writeStreamToDB(tuplestream, prop, query);
        
        DataStream<Tuple3<Double, String, Integer>> grafanastream = tuplestream
            .keyBy(value -> value.f1)
            .timeWindow(Time.seconds(1))
            .reduce(new ReduceFunction<Tuple3<Double, String, Integer>>() {
                @Override
                public Tuple3<Double, String, Integer> reduce(Tuple3<Double, String, Integer> value1, Tuple3<Double, String, Integer> value2)
                throws Exception {
                    
                    return new Tuple3(value1.f0, value1.f1, value1.f2 + value2.f2);
                }
            });
         
        String grafanaQuery = "INSERT INTO GRAFANAPATTERN (time, pattern, occurrence) VALUES (?, ?, ?);";
        writeStreamToDB(grafanastream, prop, grafanaQuery);
        /*System.out.println("start plan");
        System.out.println(env.getExecutionPlan());
        System.out.println("end plan");*/


        env.execute("");
    }
    

    public static void writeStreamToDB(DataStream<Tuple3<Double, String, Integer>> logStream, Properties prop, String query) {
        
        JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
            .setDrivername(prop.getProperty("drivername"))
            .setDBUrl(prop.getProperty("dburl"))
            .setUsername(prop.getProperty("dbusername"))
            .setPassword(prop.getProperty("dbpassword"))
            .setQuery(query)
            .setSqlTypes(new int[] { Types.TIMESTAMP, Types.VARCHAR, Types.INTEGER})
            .setBatchInterval(100)
            .finish();
        assert(jdbcOutput != null);
        DataStream<Row> rows = logStream.map((MapFunction<Tuple3<Double, String, Integer>, Row>) aTuple -> {
           Row row = new Row(3); 
           row.setField(0, new Timestamp((long)(aTuple.f0.doubleValue() * 1000))); 
           row.setField(1, aTuple.f1);
           row.setField(2, aTuple.f2);
           return row;
          });
  
        rows.writeUsingOutputFormat(jdbcOutput);
    }
}