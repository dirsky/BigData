package src.com.frank.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * @author: Guozhong Xu
 * @date: Create in 17:45 2019/8/14
 */
public class DataSetDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("DataFrameDemo");
        SparkContext sc = new SparkContext(conf);
        sc.setLogLevel("WARN");
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().format("json").load("./spark/data/input/json");

//        Dataset<Person> as = df.as(Person);


    }
}
