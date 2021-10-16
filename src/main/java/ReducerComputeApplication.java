import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class ReducerComputeApplication {
    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{
        Configuration configuration = HBaseConfiguration.create();
        //Hbase配置
        configuration.set(HConstants.ZOOKEEPER_QUORUM,"localhost:2181");
        //Mysql配置
        configuration.set(DBConfiguration.DRIVER_CLASS_PROPERTY,"com.mysql.jdbc.Driver");
        configuration.set(DBConfiguration.URL_PROPERTY,"jdbc:mysql://192.168.131.268:3306/RECRUIT?allowMutiQueries=true&amp;useSSL=false&amp;useUnicode=true");
        configuration.set(DBConfiguration.USERNAME_PROPERTY,"root");
        configuration.set(DBConfiguration.PASSWORD_PROPERTY,"Dataadt123!");
        //创建MapReduce任务对象
        Job job = Job.getInstance(configuration,"recruit");
        job.setJarByClass(ReducerComputeApplication.class);
        Scan scan = new Scan();
        //Map任务初始化 计算数据的输入
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("niubo:recruit"), scan, RecruitMapper.class, Text.class, IntWritable.class,job);
        //设置Reducer阶段的KeyOut和ValueOut的类型
        job.setOutputKeyClass(MySqlWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setReducerClass(RecruitReducer.class);
        //计算结果的输出
        job.setOutputFormatClass(DBOutputFormat.class);
        DBOutputFormat.setOutput(job,"COMPANY_COUNT","COMPANY_NAME","COUNT_NUM");
        //任务提交true输出运行日志
        job.waitForCompletion(true);
    }
}