package com.cjbdi.udfs;

import com.cjbdi.config.HbaseConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: XYH
 * @Date: 2021/12/3 5:06 下午
 * @Description: 自定义 habseSink
 */
public class HbaseSink extends RichSinkFunction<byte[]> {
    private transient Integer maxSize = 1000;
    private transient Long delayTime = 5000L;

    public HbaseSink() {
    }

    public HbaseSink(Integer maxSize, Long delayTime) {
        this.maxSize = maxSize;
        this.delayTime = delayTime;
    }

    private transient Connection connection;
    private transient Long lastInvokeTime;
    private transient List<Put> puts = new ArrayList<>(maxSize);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 获取全局配置文件，并转为ParameterTool
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        //创建一个Hbase的连接
        connection = HbaseConfig.getConnection(
                params.getRequired("hbase.zookeeper.quorum"),
                params.getInt("hbase.zookeeper.property.clientPort", 2181)
        );
        // 获取系统当前时间
        lastInvokeTime = System.currentTimeMillis();
    }

    @Override
    public void invoke(byte[] value, Context context) throws Exception {
        String rowkey = System.currentTimeMillis() + "";
        //创建put对象，并赋rowkey值
        Put put = new Put(rowkey.getBytes());
        // 添加值
        put.addColumn("doc".getBytes(), "ws".getBytes(), value);
        puts.add(put);// 添加put对象到list集合
        //使用ProcessingTime
        long currentTime = System.currentTimeMillis();
        //开始批次提交数据
        if (puts.size() == maxSize || currentTime - lastInvokeTime >= delayTime) {
            //获取一个Hbase表
            Table table = connection.getTable(TableName.valueOf("ns_ws:t_ws_test"));
            table.put(puts);//批次提交
            puts.clear();
            lastInvokeTime = currentTime;
            table.close();
        }
    }
}
