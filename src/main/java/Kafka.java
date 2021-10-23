import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class Kafka {
    public static void main(String[] args) {

        // 在上次实验的java类中添加HBase相关配置信息
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, "localhost:2181");
        Table table = null;
        try {
            // 创建连接HBase实例
            Connection connection = ConnectionFactory.createConnection(conf);

            // 将读取的数据设置放入指定表中
            table = connection.getTable(TableName.valueOf("niubo:recruit"));
        } catch (IOException e) {
            e.printStackTrace();
        }

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
                // 将拆分的数据放入Put对象中
                Put put = new Put(Bytes.toBytes(recruit.getID()));
                // 为Put对象中的数据指定列簇与列名
                if (recruit.getCompanyName()==null){
                    continue;
                }
                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("companyName"),Bytes.toBytes(recruit.getCompanyName()));
                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("jobSalary"),Bytes.toBytes(recruit.getJOB_SALARY()));
                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("maxSalary"),Bytes.toBytes(recruit.getMaxSalary()));
                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("minSalary"),Bytes.toBytes(recruit.getMinSalary()));

                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("positionName"),Bytes.toBytes(recruit.getPositionName()));

                // 将处理后的Put对象添加到集合中
                try {
                    table.put(put);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
            consumer.commitAsync();
        }
    }

}
