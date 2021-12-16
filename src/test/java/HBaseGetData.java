import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.FileOutputStream;

/**
 * @Author: XYH
 * @Date: 2021/11/11 11:24 上午
 * @Description: 根据 rowkey 查询数据从 HBase 中 get 数据并保存到 Linux 本地目录
 */
public class HBaseGetData {

    static Configuration conf = null;
    static Connection conn = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "rookiex01");
        conf.set("hbase.zookeeper.property.client", "2181");

        try{
            conn = ConnectionFactory.createConnection(conf);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void getResult(TableName tableName, String rowKey) throws Exception{
        Table table = conn.getTable(tableName);
        //获得一行
        Get get = new Get(Bytes.toBytes(rowKey));
        Result rs = table.get(get);
        //保存 get result 的结果, 字节数组的形式
        byte[] bs = rs.value();
        table.close();
        File file = new File("/data/ws.doc");
        FileOutputStream fos = new FileOutputStream(file);
        fos.write(bs);
        fos.close();
//        Result set = table.get(get);
//        Cell[] cells = set.rawCells();
//        for (Cell cell: cells){
//            System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
//                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
//        }
//        System.out.println("----------------****************_____________________");
//        table.close();

    }


    //扫描全表
    public static void scanTable(TableName tableName) throws Exception{
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
//        //可以做筛选
//        scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"));
//        scan.addFamily(Bytes.toBytes("f1"));
//        scan.setStartRow(scan.getStartRow());
//        scan.setStopRow(scan.getStopRow());
//        scan.setTimestamp(timestamp);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("rowkey: " + Bytes.toString((CellUtil.cloneRow(cell))));
                System.out.println("列族: " + Bytes.toString((CellUtil.cloneFamily(cell))));
                System.out.println("列: " + Bytes.toString((CellUtil.cloneQualifier(cell))));
                System.out.println("值: " + Bytes.toString((CellUtil.cloneValue(cell))));
                System.out.println("时间戳: " + cell.getTimestamp());
                System.out.println("*********************************************");
            }
        }
    }
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        HBaseGetData.getResult(TableName.valueOf("t_ws"), "1638606155786");
    //    HBaseGetData.scanTable(TableName.valueOf("ns_xyh:t_doc"));
        long endTime = System.currentTimeMillis();
        System.out.println("HBase get 数据共耗时: " + (endTime-startTime));
    }

}
