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
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class SparkRDD {
    public static void main(String[] args) throws SQLException {
        // 创建JavaSparkContext对象
        SparkConf sparkConf = new SparkConf().setAppName("Spark2MySql")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //配置HBase链接信息
        Configuration hConf = HBaseConfiguration.create();
        hConf.set(HConstants.ZOOKEEPER_QUORUM,"localhost:2181");
        hConf.set(TableInputFormat.INPUT_TABLE,"niubo:recruit");
        //组装Java RDD
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRead = sc.newAPIHadoopRDD(hConf,
                TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaRDD<Recruit> recruitJavaRDD = hbaseRead.map(t -> {
            String companyName = Bytes.toString(t._2.getValue("cf1".getBytes(),"companyName".getBytes()));
            return new Recruit(companyName);
        });

        JavaPairRDD<String,Integer> counts = recruitJavaRDD.flatMap(s -> Arrays.asList(s.getCompanyName()).iterator())
                //key是companyName，value是1
                .mapToPair(companyName -> new Tuple2<>(companyName,1))
                //基于key进行reduce，逻辑是将value累加
                .reduceByKey((a,b) ->a+b);

        //先将key和value倒过来，再按照key排序
        JavaPairRDD<Integer,String> sorts = counts
                //key和value颠倒，生成新的map
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._2(),tuple2._1()))
                //按照key倒序排序
                .sortByKey(false);

        //取前10个
        List<Tuple2<Integer, String>> top10 = sorts.take(10);

        Connection conn = null;
        PreparedStatement ps = null;
        String url;
        try {
        //1.加载驱动程序
        Class.forName("com.mysql.cj.jdbc.Driver");
        //2.获得数据库的连接
        url = "jdbc:mysql://localhost:3306/RECRUIT";
        conn = DriverManager.getConnection(url, "root","7wtB6{W?f(vxtVkM");
        conn.setAutoCommit(false);
        String sql = "insert into COMPANY_COUNT(COMPANY_NAME,COUNT_NUM) values (?,?)";
        ps = conn.prepareStatement(sql);
        for(Tuple2<Integer,String> tuple2 : top10){
            ps.setString(1,tuple2._2);
            ps.setInt(2,tuple2._1);
            ps.executeUpdate();
        }
        conn.commit();

        }catch (Exception e){
            e.printStackTrace();
            try {
                conn.rollback();
            }catch (SQLException ex){
                ex.printStackTrace();
            }
        }finally {
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
}
