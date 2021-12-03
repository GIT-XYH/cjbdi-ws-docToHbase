package com.cjbdi.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Properties;

/**
 * @Author: XYH
 * @Date: 2021/12/3 1:10 下午
 * @Description: kafka相关配置
 */
public class KafkaConfig {

    public static String brokers;
    public static String groupId;
    public static String inputTopic;
    public static String outputTopic;
    public static String jsonErrorTopic;
    public static String analysisErrorTopic;
    public static String toKafkaErrorTopic;

    public static void KafkaEnv(ParameterTool parameterTool) {
        //配置kafka参数设置
        Properties properties = new Properties();
        properties.setProperty("max.request.size", "214748364");
        properties.setProperty("compression.type", "gzip");
        properties.setProperty("buffer.memory", "335544320");
        properties.setProperty("batch.size", "1638400");
        properties.setProperty("max.block.ms", "214748364");

        //指定 kafka topic 等相关参数
        brokers = parameterTool.getRequired("bootstrap-servers");
        inputTopic = parameterTool.getRequired("input-topic");
        groupId = parameterTool.getRequired("input-group-id");
        jsonErrorTopic = parameterTool.getRequired("json-error-topic");
        analysisErrorTopic = parameterTool.getRequired("analysis-error-topic");
        outputTopic = parameterTool.getRequired("output-topic");
        toKafkaErrorTopic = parameterTool.getRequired("toKafka-error-topic");
    }
}
