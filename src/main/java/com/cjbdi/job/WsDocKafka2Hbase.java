package com.cjbdi.job;

import com.cjbdi.bean.WsBeanFromKafka;
import com.cjbdi.config.FlinkConfig;
import com.cjbdi.config.KafkaConfig;
import com.cjbdi.udfs.HbaseSink;
import com.cjbdi.udfs.WsSourceJsonToWsBeanFunction;
import com.cjbdi.udfs.WsToHbaseProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import static com.cjbdi.config.FlinkConfig.*;

/**
 * @Author: XYH
 * @Date: 2021/12/3 12:53 下午
 * @Description: 在 kafka 中获取到文书 json, 提取出其中的原文本, 存入 hbase
 */
public class WsDocKafka2Hbase {
    public static void main(String[] args) throws Exception {
        //获取全局参数
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);
        //配置 flink 的运行环境
        FlinkConfig.flinkEnv(parameterTool);
        //配置 kafka 相关信息
        KafkaConfig.KafkaEnv(parameterTool);
        //传递全局参数
        env.getConfig().setGlobalJobParameters(parameterTool);
        //从指定的 kafkaSource 中读取数据
        DataStreamSource<String> kafkaStream = FlinkConfig.env.fromSource(FlinkConfig.kafkaSource, WatermarkStrategy.noWatermarks(), "ws-source");
        //将 kafka 中的 json 转换成 bean
        SingleOutputStreamOperator<WsBeanFromKafka> wsBeanStream = kafkaStream.process(new WsSourceJsonToWsBeanFunction(jsonErrorData));
        //输出 json 解析失败的数据
        DataStream<String> jsonErrorStream = wsBeanStream.getSideOutput(jsonErrorData);
        //将 json 解析失败的数据放到 kafka 指定的 topic 中
        jsonErrorStream.sinkTo(jsonErrorSink);
        //取出文书中的初始文书(转成二进制数组类型)
        SingleOutputStreamOperator<byte[]> writeToHbaseStream = wsBeanStream.process(new WsToHbaseProcessFunction(writeErrorData));
        //将解析好的文书存储到 habse 中
        writeToHbaseStream.addSink(new HbaseSink());
        env.execute();
    }
}
