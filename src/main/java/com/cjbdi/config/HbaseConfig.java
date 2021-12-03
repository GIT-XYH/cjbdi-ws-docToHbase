package com.cjbdi.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

/**
 * @Author: XYH
 * @Date: 2021/12/3 4:35 下午
 * @Description: TODO
 */
public class HbaseConfig {
    public static Connection getConnection(String zkQuorum, int port) throws Exception {
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "rookiex01,rookiex02,rookiex03");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConf.set("hbase.client.keyvalue.maxsize", "102400000");
        Connection conn = ConnectionFactory.createConnection(hbaseConf);
        return conn;
    }
}
