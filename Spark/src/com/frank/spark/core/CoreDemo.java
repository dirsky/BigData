package com.frank.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author: Guozhong Xu
 * @date: Create in 17:45 2019/8/14
 */
public class CoreDemo {
    public static void main(String[] args) {
        System.out.println("-----------------创建conf----------------------");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD");
        System.out.println("-----------------创建JavaSparkContext ---------");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        System.out.println("-----------------读取sc.textFile--------------");
        JavaRDD<String> lines = sc.textFile("./data/input/words-sort.txt");
        JavaRDD<String> union = lines.union(lines);
        JavaRDD<String> stringJavaRDD = union.sortBy(s -> s, true, 1);
        stringJavaRDD.foreach(s -> System.out.println(s));
        // 下面的行因为序列化问题不能运行
//        stringJavaRDD.foreach(System.out::println);
        JavaPairRDD<String, Integer> pairRDD = stringJavaRDD.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> reduceByKey = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        reduceByKey.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });


        sc.stop();
    }
}
