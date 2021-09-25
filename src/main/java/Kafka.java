import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class Kafka {
    public static void main(String[] args) {
        // 1. 创建配置对象 指定Consumer信息
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,IntegerDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");

        // 关闭kafka consumer的offset自动提交功能
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        // 2. 创建消费者
        KafkaConsumer<Integer,String> consumer = new KafkaConsumer<>(properties);
        // 3. 订阅主题
        consumer.subscribe(Arrays.asList("test"));
        // 4. 拉取主题内新增的数据
        while (true){
            ConsumerRecords<Integer,String> records = consumer.poll(5L);

            for (ConsumerRecord<Integer,String> record : records){
                Recruit recruit = JSONObject.parseObject(record.value(), Recruit.class);
                String[] split = recruit.getJOB_SALARY().replaceAll("k","000")
                        .split("-");
                    recruit.setMinSalary(split[0]);
                    recruit.setMaxSalary(split.length==1?split[0]:split[1]);
                System.out.println(recruit.toString());
            }
            consumer.commitAsync();
        }
    }

}
