package com.frank.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author: Guozhong Xu
 * @date: Create in 10:56 2019/8/12
 */
public class HBaseUtil {

    private HBaseAdmin admin;
    private String  tbName;
    HTable htable;
    Random r = new Random();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    public HBaseUtil(String tName) throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node101");
        admin = new HBaseAdmin(conf);
        htable = new HTable(conf, tName.getBytes());
        this.tbName = tName;
    }

    public void createTable(String tName, String[] colFamily) throws IOException {
        if (null != tName) {
            this.tbName = tName;
        }
        if (admin.tableExists(tbName)) {
            admin.disableTable(tbName);
            admin.deleteTable(tbName);
        }
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tbName));
        for (String cf : colFamily) {
            HColumnDescriptor family = new HColumnDescriptor(cf.getBytes());
            desc.addFamily(family);
        }
        admin.createTable(desc);
    }


    public void insert(String key, String[][] content) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        Put put = new Put(key.getBytes());
        for (String[] lists : content) {
            put.add(lists[0].getBytes(), lists[1].getBytes(), lists[2].getBytes());
        }
        htable.put(put);
    }

    /**
     * 有10个用户，每个用户随机产生100条记录
     *
     * @throws Exception
     */
    public void insertDB2() throws Exception {
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 2; i++) {
            String phoneNum = getPhoneNum("137");
            for (int j = 0; j < 3; j++) {
                String dnum = getPhoneNum("150");
                String length = r.nextInt(99) + "";
                String type = r.nextInt(2) + "";
                String dateStr = getDate("2018");
                String rowkey = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse(dateStr).getTime());
//                System.out.println(Long.MAX_VALUE + "-" + sdf.parse(dateStr).getTime() + "-" + rowkey);
                Put put = new Put(rowkey.getBytes());
                put.add("cf2".getBytes(), "dnum".getBytes(), dnum.getBytes());
                put.add("cf2".getBytes(), "length".getBytes(), length.getBytes());
                put.add("cf2".getBytes(), "type".getBytes(), type.getBytes());
                put.add("cf2".getBytes(), "date".getBytes(), dateStr.getBytes());
                puts.add(put);
            }
        }
        htable.put(puts);
    }


    /**
     * 生成随机的手机号码
     *
     * @param string
     * @return
     */
    private String getPhoneNum(String string) {
        return string + String.format("%08d", r.nextInt(99999999));
    }

    private String getDate(String year) {
        return year + String.format("%02d%02d%02d%02d%02d",
                new Object[] { r.nextInt(12) + 1, r.nextInt(31) + 1, r.nextInt(24), r.nextInt(60), r.nextInt(60) });
    }

}
