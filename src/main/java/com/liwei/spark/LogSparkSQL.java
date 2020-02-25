package com.liwei.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;

public class LogSparkSQL {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        //SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> lines = sc.textFile("D:\\result.txt");
        //将字符串转换成LogInfoRDD
        JavaRDD<LogInfo> logInfo = lines.map(line -> {
            String[] str = line.split(",");
            long timeStamp = Long.valueOf(str[0]);
            String phone = str[1];
            long down = Long.valueOf(str[2]);
            long up = Long.valueOf(str[3]);
            LogInfo log = new LogInfo(down, phone, timeStamp, up);
            return log;
        });

        //将RDD转换成DataSet数据集
        //Dataset<Row> df = sqlContext.createDataFrame(logInfo, LogInfo.class);
        Dataset<Row> df = session.createDataFrame(logInfo,LogInfo.class);
        //将df注册成临时视图，这样可以用SQL表达式进行查询操作
        df.createOrReplaceTempView("log");
        //Dataset<Row> dataset = sqlContext.sql("select * from log where up > 1000 limit 5");
        Dataset<Row> dataset = session.sql("select * from log where up > 1000 limit 5");
        Row row[] = (Row[]) dataset.collect();
        List<LogInfo> list = new ArrayList<>();
        for (int i = 0; i < row.length; i++) {
            LogInfo info = new LogInfo();
            info.setDown(row[i].getLong(0));
            info.setPhoneNo(row[i].getString(1));
            info.setTimeStamp(row[i].getLong(2));
            info.setUp(row[i].getLong(3));
            list.add(info);
        }
        dataset.show();

    }

}
