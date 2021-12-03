package com.cjbdi.udfs;

import com.alibaba.fastjson.JSON;
import com.cjbdi.bean.WsBean;
import com.cjbdi.bean.WsBeanFromKafka;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Base64;

/**
 * @Author: XYH
 * @Date: 2021/12/3 4:54 下午
 * @Description: TODO
 */
public class WsToHbaseProcessFunction extends ProcessFunction<WsBeanFromKafka, byte[]> {
    private OutputTag<String> writeErrorData;
    public WsToHbaseProcessFunction(OutputTag<String> outputTag) {
        this.writeErrorData = outputTag;
    }
    @Override
    public void processElement(WsBeanFromKafka wsBeanFromKafka, Context ctx, Collector<byte[]> out) throws Exception {
        /**
         * 先将 base64格式的文本转成二进制的文本
         */
        try {
            WsBean wsBean = wsBeanFromKafka.getWsBean();
            String base64File = wsBeanFromKafka.getBase64File();
            //wsDoc 就是二进制格式的文书原文件
            byte[] wsDoc = Base64.getDecoder().decode(base64File);
        } catch (Exception e) {
            e.printStackTrace();
            ctx.output(writeErrorData, JSON.toJSONString(wsBeanFromKafka));
        }

    }
}
