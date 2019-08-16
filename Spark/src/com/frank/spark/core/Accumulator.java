package com.frank.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.List;

/**
 * @author: Guozhong Xu
 * @date: Create in 17:45 2019/8/14
 */
public class Accumulator {
    public static void main(String[] args) {
        System.out.println("-----------------创建conf----------------------");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD");
        System.out.println("-----------------创建JavaSparkContext ---------");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        System.out.println("-----------------读取sc.textFile--------------");
        JavaRDD<String> lines = sc.textFile("./data/input/data-reduce.txt");

//        org.apache.spark.DataSetDemo<Integer> blanklines = sc.intAccumulator(0);
//        JavaRDD<Integer> map = lines.map(new Function<String, Integer>() {
//            @Override
//            public Integer call(String s) throws Exception {
//                if (s == "") {
//                    blanklines += 1;
//                }
//                return null;
//            }
//        });


        sc.stop();
    }
}
