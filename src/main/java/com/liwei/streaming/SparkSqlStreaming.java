package com.liwei.streaming;

import com.liwei.spark.LogInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import java.util.*;

public class SparkSqlStreaming {
    public static void main(String[] args) throws InterruptedException {

        JavaSparkContext sc;
        JavaStreamingContext ssc;
        Map<String, Object> kafkaParams = new HashMap<>();
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("spark");
        sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        ssc = new JavaStreamingContext(sc, Durations.seconds(2));

        Collection<String> topicsSet = new HashSet<>(Arrays.asList("log3"));
        //kafka相关参数，必要！缺了会报错
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "group1");
        kafkaParams.put("enable.auto.commit", false);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);

        //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        JavaInputDStream<ConsumerRecord<Object, Object>> lines = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
        );

        lines.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<Object, Object>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<Object, Object>> consumerRecordJavaRDD) throws Exception {
                JavaRDD<LogInfo> rdd = consumerRecordJavaRDD.map(new Function<ConsumerRecord<Object, Object>, LogInfo>() {
                    @Override
                    public LogInfo call(ConsumerRecord<Object, Object> objectObjectConsumerRecord) throws Exception {
                        String data[] = objectObjectConsumerRecord.value().toString().split(",");
                        long timeStamp = Long.valueOf(data[0]);
                        String phone = data[1];
                        long down = Long.valueOf(data[2]);
                        long up = Long.valueOf(data[3]);
                        LogInfo log = new LogInfo(down, phone, timeStamp, up);
                        return log;
                    }
                });

                Dataset<Row> df = session.createDataFrame(rdd, LogInfo.class);
                df.createOrReplaceTempView("log");
                Dataset<Row> dataset = session.sql("select * from log where up > 1000 limit 5");
                dataset.show();
            }
        });
        ssc.start();
        ssc.awaitTermination();
        ssc.close();
    }
}
