package com.liwei.spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import java.util.Arrays;
import java.util.List;

public class SparkRDD {
    private static Logger logger = Logger.getLogger(SparkRDD.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        //FirstRDD
        JavaPairRDD<Integer, Integer> firstRDD = rdd.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer num) throws Exception {
                logger.debug("**************************************************************"+num);
                return new Tuple2<Integer, Integer>(num, num * num);
            }
        });

        firstRDD.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                System.out.println(integerIntegerTuple2._1+":"+integerIntegerTuple2._2);
            }
        });
        //sRDD
        JavaPairRDD<Integer, String> sRDD = rdd.mapToPair(new PairFunction<Integer, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Integer num) throws Exception {
                return new Tuple2<Integer, String>(num, String.valueOf((char) 64 + num * num));
            }
        });
        JavaPairRDD<Integer, Tuple2<Integer, String>> joinRDD = firstRDD.join(sRDD);
        JavaRDD<String> res = joinRDD.map((Function<Tuple2<Integer, Tuple2<Integer, String>>, String>) v1 -> {
            int key = v1._1;
            int value1 = v1._2._1;
            String value2 = v1._2._2;
            return "<" + key + ",<" + value1 + "," + value2 + ">>";
        });
    }
}
