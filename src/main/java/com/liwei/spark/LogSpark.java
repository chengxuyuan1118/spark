package com.liwei.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class LogSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("spark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        String path = "D:\\result.txt";
        JavaRDD<String> lines = sc.textFile(path);
        JavaPairRDD<String, LogInfo> logPairRDD = RDD2RDDPair(lines);
        JavaPairRDD<String, LogInfo> reduceByKey = aggregateByDeviceID(logPairRDD);
        reduceByKey.foreach(x -> System.out.println(x._2.getDown()));
        System.out.println(reduceByKey.count());
        sc.close();
    }

    //实现strRDD到LogInfo RDD的转换   <K,V> 电话号为K，LogInfor为V
    private static JavaPairRDD<String, LogInfo> RDD2RDDPair(JavaRDD<String> accessLogRDD) {
        return accessLogRDD.mapToPair((PairFunction<String, String, LogInfo>) line -> {
            String[] logInfo = line.split(",");
            long timeStamp = Long.valueOf(logInfo[0]);
            String phone = logInfo[1];
            long down = Long.valueOf(logInfo[2]);
            long up = Long.valueOf(logInfo[2]);
            LogInfo log = new LogInfo(timeStamp, phone, down, up);
            return new Tuple2<String, LogInfo>(phone, log);
        });
    }

    //实现reduceByKey 电话号为K，将上行流量和下行流量加和
    private static JavaPairRDD<String, LogInfo> aggregateByDeviceID(JavaPairRDD<String, LogInfo> pairRDD) {
        return pairRDD.reduceByKey((Function2<LogInfo, LogInfo, LogInfo>) (v1, v2) -> {

                    //时间戳为最早的时间
                    long timeStamp = v1.getTimeStamp() < v2.getTimeStamp() ? v1.getTimeStamp() : v2.getTimeStamp();
                    //上行流量和下行流量进行add
                    long up = v1.getUp() + v2.getUp();
                    long down = v1.getDown() + v2.getDown();
                    String phone = v1.getPhoneNo();
                    return new LogInfo(down, phone,timeStamp , up);
                }
        );
    }

}
