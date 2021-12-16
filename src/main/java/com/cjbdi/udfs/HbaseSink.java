package com.cjbdi.udfs;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @Author: XYH
 * @Date: 2021/12/3 5:06 下午
 * @Description: 自定义 habseSink
 */
public class HbaseSink  extends RichSinkFunction<byte[]> {
    private transient List<Put> puts = new ArrayList<>();
    private ParameterTool jobParameters;
    int a = 0;
    Connection conn;
    Table table;
    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        jobParameters = (ParameterTool)getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", jobParameters.getRequired("querum"));
//        hbaseConf.set("hbase.zookeeper.quorum", "192.158.5.125");
//        hbaseConf.set("hbase.zookeeper.quorum", "rookiex01");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConf.set("hbase.client.keyvalue.maxsize", "102400000");
        conn = ConnectionFactory.createConnection(hbaseConf);
        table = (Table) conn.getTable(TableName.valueOf(jobParameters.getRequired("table-name")));

    }

    //    private transient Integer maxSize = 1000;
//    private transient Long delayTime = 5000L;
//
//    public HbaseSink() {
//    }
//
//    public HbaseSink(Integer maxSize, Long delayTime) {
//        this.maxSize = maxSize;
//        this.delayTime = delayTime;
//    }
//
//    private transient Connection connection;
//    private transient Long lastInvokeTime;
//    private transient List<Put> puts = new ArrayList<>(maxSize);
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        //创建一个Hbase的连接
//        connection = HbaseConfig.getConnection();
//        // 获取系统当前时间
//        lastInvokeTime = System.currentTimeMillis();
//    }
//
//    @Override
//    public void invoke(byte[] value, Context context) throws Exception {
//        String rowkey = System.currentTimeMillis() + "";
//        //创建put对象，并赋rowkey值
//        Put put = new Put(rowkey.getBytes());
//        // 添加值
//        put.addColumn("ws_doc".getBytes(), "ws".getBytes(), value);
//        puts.add(put);// 添加put对象到list集合
//        //使用ProcessingTime
//        long currentTime = System.currentTimeMillis();
//        //开始批次提交数据
//        if (puts.size() == maxSize || currentTime - lastInvokeTime >= delayTime) {
//            //获取一个Hbase表
//            Table table = connection.getTable(TableName.valueOf("t_ws"));
//            table.put(puts);//批次提交
//            puts.clear();
//            lastInvokeTime = currentTime;
//            table.close();
//        }
//    }
    @Override
    public void invoke(byte[] value, Context context) throws Exception {
        String rowKey = new Date().getTime() + "" + a;
        a++;
        System.out.println("rowKey 为: " + rowKey);
        //指定ROWKEY的值
        Put put = new Put(Bytes.toBytes(rowKey));
        //指定列簇名称、列修饰符、列值 temp.getBytes()
        put.addColumn(jobParameters.getRequired("table-cf").getBytes(), "ws".getBytes(), value);
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
