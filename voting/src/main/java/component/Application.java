package component;

import kafka.serializer.StringDecoder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

public class Application {


    private long windowDuration = 8000L;
    private boolean sortByFrquency = false;
    private static String topic = "voting";
    private static String broker = "ip-172-31-20-247.ec2.internal:6667";
    private boolean filterFire = false;


    public static JavaStreamingContext processStream() throws SparkException {

        // create a local Spark Context with two working threads
        SparkConf streamingConf = new SparkConf().setMaster("local[2]").setAppName("voting");
        streamingConf.set("spark.streaming.stopGracefullyOnShutdown", "true");

        // create a Spark Java streaming context, with stream batch interval
        JavaStreamingContext jssc = new JavaStreamingContext(streamingConf, Durations.milliseconds(1000));

        // set checkpoint for demo
        jssc.checkpoint(System.getProperty("java.io.tmpdir"));

        jssc.sparkContext().setLogLevel(Level.OFF.toString()); // turn off Spark logging

        // set up receive data stream from kafka
        final Set<String> topicsSet = new HashSet<>(Arrays.asList(topic));
        final Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", broker);

        // create a Discretized Stream of Java String objects
        // as a direct stream from kafka (zookeeper is not an intermediate)
        JavaPairInputDStream<String, String> rawDataLines =
                KafkaUtils.createDirectStream(
                        jssc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topicsSet
                );

        JavaDStream<String> lines = rawDataLines.map((Function<Tuple2<String, String>, String>) Tuple2::_2);
        System.out.println(lines);








        return jssc;
    }

    public static void main(String[] args) throws InterruptedException {
        // Configure and initialize the SparkStreamingContext
        try {
            processStream();
        }
        catch (Exception e){
            System.out.println(e.getCause());
        }

    }
}

