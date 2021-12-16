import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: XYH
 * @Date: 2021/12/4 3:12 下午
 * @Description: TODO
 */
public class HbaseTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "rookiex1:9092");


        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        //从最新开始消费
        consumer.setStartFromLatest();
        DataStream<String> stream = env.addSource(consumer);
        stream.print();
//        stream.addSink(new HbaseSink());
        //stream.map();
        env.execute();
    }
}
