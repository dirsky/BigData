package com.frank.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author: Guozhong Xu
 * @date: Create in 17:01 2019/8/13
 */
public class SparkStreamingTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]").setAppName("SparkStreamingTest");

        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        JavaStreamingContext jsc = new JavaStreamingContext(sc,Durations.seconds(5));
        // 启动socket server 服务器：nc -lk 9999
        JavaReceiverInputDStream<String> node1 = jsc.socketTextStream("node101", 9999);

        JavaDStream<String> words = node1.flatMap((FlatMapFunction<String, String>) line
                -> Arrays.asList(line.split(" ")));
        JavaPairDStream<String, Integer> pairWords = words.mapToPair((PairFunction<String, String, Integer>) word
                -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> result = pairWords.reduceByKey((Function2<Integer, Integer, Integer>) (w1, w2)
                -> w1 + w2);

        result.print();
        result.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
                System.out.println("Driver .......");
                SparkContext context = rdd.context();
                JavaSparkContext javaSparkContext = new JavaSparkContext(context);
                // 可以在此处从文件拿“hello”或者list 再在call下进行处理
                Broadcast<String> broadcast = javaSparkContext.broadcast("hello");
                String value = broadcast.value();
                System.out.println(value);

                JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
                        System.out.println("Executor .......");
                        return new Tuple2<String, Integer>(tuple._1, tuple._2);
                    }
                });
                pairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        System.out.println(stringIntegerTuple2);
                    }
                });
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.stop();

    }
}
