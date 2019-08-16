package com.frank.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.List;

/**
 * @author: Guozhong Xu
 * @date: Create in 17:45 2019/8/14
 */
public class ActionDemo {
    public static void main(String[] args) {
        System.out.println("-----------------创建conf----------------------");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD");
        System.out.println("-----------------创建JavaSparkContext ---------");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        System.out.println("-----------------读取sc.textFile--------------");
        JavaRDD<String> lines = sc.textFile("./data/input/data-reduce.txt");
        String reduce = lines.reduce(
                (s, s2) -> String.valueOf(Integer.parseInt(s) + Integer.parseInt(s2))
        );
        System.out.println("和：" + reduce);

        long count = lines.count();
        System.out.println("数量："+count);

        String first = lines.first();
        System.out.println("第一个元素："+first);

        System.out.println("take前3个");
        List<String> take = lines.take(3);
        take.forEach(System.out::println);

        System.out.println("takeSample抽样3个");
        List<String> takeSample = lines.takeSample(true,3);
        takeSample.forEach(System.out::println);


        sc.stop();
    }
}
