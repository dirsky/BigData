package com.frank.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * map, flatMap, mapToPair, foreach
 * @author: Guozhong Xu
 * @date: Create in 16:51 2019/8/14
 */
public class RDD {
    public static void main(String[] args) {
        System.out.println("-----------------创建conf----------------------");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD");
        System.out.println("-----------------创建JavaSparkContext ---------");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        System.out.println("-----------------读取sc.textFile--------------");
        JavaRDD<String> lines = sc.textFile("./data/input/words");

        System.out.println("-----------------lines.foreach---------------------");
        lines.foreach(s -> System.out.println(s));

        // map一对一
        JavaRDD<String> map = lines.map(s -> "my" + s);
        System.out.println("---------List<String> collect = lines.collect()----------");
        List<String> collect1 = map.collect();
        collect1.forEach(System.out::println);

        // map一行转多行
        JavaRDD<String> word = map.flatMap(
                s -> Arrays.asList(s.split(" "))
        );
        JavaRDD<String> wordFrank = word.filter(s -> s.contains("frank"));
        JavaRDD<String> distinct = wordFrank.distinct();

        // map一对二
        JavaPairRDD<String, Integer> pairRDD = distinct.mapToPair(
                s -> new Tuple2<>(s, 1)
        );

        System.out.println("---------List<String> collect2 = lines.collect()----------");
        List<Tuple2<String, Integer>> collect2 = pairRDD.collect();
        collect2.forEach(System.out::println);

        System.out.println("----------------------------------------------");

        sc.stop();
    }
}
