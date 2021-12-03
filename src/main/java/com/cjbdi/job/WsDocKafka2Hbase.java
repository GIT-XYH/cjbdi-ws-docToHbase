package com.cjbdi.job;

import com.cjbdi.bean.WsBeanFromKafka;
import com.cjbdi.config.FlinkConfig;
import com.cjbdi.config.KafkaConfig;
import com.cjbdi.udfs.HbaseSink;
import com.cjbdi.udfs.WsSourceJsonToWsBeanFunction;
import com.cjbdi.udfs.WsToHbaseProcessFunction;
import com.sun.xml.internal.ws.encoding.HasEncoding;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Properties;

import static com.cjbdi.config.FlinkConfig.*;

/**
 * @Author: XYH
 * @Date: 2021/12/3 12:53 下午
 * @Description: 在 kafka 中获取到文书 json, 提取出其中的原文本, 存入 hbase
 */
public class WsDocKafka2Hbase {
    public static void main(String[] args) throws IOException {

        //获取全局参数
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("/Users/xuyuanhang/Desktop/code/cjbdi-ws-docToKafka/src/main/resources/parameter.properties");
        //配置 flink 的运行环境
        FlinkConfig.flinkEnv(parameterTool);
        //配置 kafka 相关信息
        KafkaConfig.KafkaEnv(parameterTool);
        //从指定的 kafkaSource 中读取数据
        DataStreamSource<String> kafkaStream = FlinkConfig.env.fromSource(FlinkConfig.kafkaSource, WatermarkStrategy.noWatermarks(), "ws-source");
        //将 kafka 中的 json 转换成 bean
        SingleOutputStreamOperator<WsBeanFromKafka> wsBeanStream = kafkaStream.process(new WsSourceJsonToWsBeanFunction(jsonErrorData));
        //输出 json 解析失败的数据
        DataStream<String> jsonErrorStream = wsBeanStream.getSideOutput(jsonErrorData);
        //将 json 解析失败的数据放到 kafka 指定的 topic 中
        jsonErrorStream.sinkTo(jsonErrorSink);
        //取出文书中的初始文书, 将初始文书存入 habse 中
        SingleOutputStreamOperator<byte[]> writeToHbaseStream = wsBeanStream.process(new WsToHbaseProcessFunction(writeErrorData));
        //将原始文书输出到 habse 中
        HbaseSink hbaseSink = new HbaseSink();
    }
}
