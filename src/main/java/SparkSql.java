import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SparkSql {
    public static void main(String[] args){
        //创建SparkSession对象
        SparkConf sparkConf = new SparkConf().setAppName("Spark2Mysql")
                .setMaster("local[*]");
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        //创建JavaSparkContext对象
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        //配置HBase链接信息
        Configuration hConf = HBaseConfiguration.create();
        hConf.set(HConstants.ZOOKEEPER_QUORUM,"localhost:2181");
        hConf.set(TableInputFormat.INPUT_TABLE,"niubo:recruit");
        //组装JAVA RDD
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRead = sc.newAPIHadoopRDD(hConf,
                TableInputFormat.class,ImmutableBytesWritable.class,Result.class);
        JavaRDD<Recruit> recruitJavaRDD = hbaseRead.map(t -> {
            Long id = Bytes.toLong(t._1.get());
            String companyName = Bytes.toString(t._2.getValue("cf1".getBytes(),"companyName".getBytes()));
            String positionName = Bytes.toString(t._2.getValue("cf1".getBytes(),"positionName".getBytes()));

            String jobSalary = Bytes.toString(t._2.getValue("cf1".getBytes(),"jobSalary".getBytes()));
            return new Recruit(companyName,positionName,jobSalary);
        });

        Dataset<Row> dataFrame = spark.createDataFrame(recruitJavaRDD,Recruit.class);
        dataFrame.toDF()
                .createOrReplaceTempView("recruit");
        spark.sql("select * from recruit")
                .show();

        Properties prop = new Properties();
        prop.setProperty("user","root");
        prop.setProperty("password","7wtB6{W?f(vxtVkM");
        spark.sql("select positionName as COUNT_NAME,count(companyName) as COUNT_NUM," +
                        "'岗位' as ITEM,1 as ITEM_FLAG from recruit " +
                        "where positionName is not null " +
                        "group by positionName " +
                        "order by count_num desc " +
                        "limit 10")
                .write().mode("append").jdbc("jdbc:mysql://localhost:3306/RECRUIT","RECRUIT_COUNT",prop);

        spark.sql("select companyName as COUNT_NAME,count(JOB_SALARY) as COUNT_NUM," +
                        "'公司' as ITEM,2 as ITEM_FLAG from recruit " +
                        "group by companyName " +
                        "order by count_num desc " +
                        "limit 10")
                .write().mode("append").jdbc("jdbc:mysql://localhost:3306/RECRUIT","RECRUIT_COUNT",prop);
    }

}