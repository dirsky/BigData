package src.com.frank.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * @author: Guozhong Xu
 * @date: Create in 15:35 2019/8/13
 */
public class DataFrameDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("DataFrameDemo");
        SparkContext sc = new SparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);
        org.apache.spark.sql.DataFrame df = sqlContext.read().format("json").load("./spark/data/input/json");
        // 读取完毕列根据ASSIC码排序

//        DataFrameDemo df = sqlContext.read().load("./json");
        df.show();
        df.printSchema();
        df.write().mode(SaveMode.Overwrite).parquet("./spark/data/output/save.df");

        org.apache.spark.sql.DataFrame load = sqlContext.read().parquet("./spark/data/output/save.df");
        System.out.println("load.show()");
        load.show();


        // 通过原生的方式
        org.apache.spark.sql.DataFrame df2 = df.select("name", "age").where("age>10");
        df2.show();
        df.printSchema();

        // 通过sql的方式 tableName只是一个指向
        df.registerTempTable("df");
        org.apache.spark.sql.DataFrame df3 = sqlContext.sql("select name,age from df where age>1");
        df3.show();
        df.printSchema();

        sc.stop();
    }
}
