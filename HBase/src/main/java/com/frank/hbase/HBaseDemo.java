package com.frank.hbase;


/**
 * @author: Guozhong Xu
 * @date: Create in 7:29 2019/8/12
 */
public class HBaseDemo {
    public static void main(String[] args) throws Exception {
        HBaseUtil hBaseUtil = new HBaseUtil("phone");
        hBaseUtil.createTable(null,
                new String[]{"cf1","cf2"});

        String[][] input = {
                {"cf1","name","frank"},
                {"cf1","age","18"},
                {"cf1","sex","boy"}};

        hBaseUtil.insert("333", input);
        hBaseUtil.insertDB2();

    }
}
