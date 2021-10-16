import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSqlTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("wordCount")
                .setMaster("local[*]");
        SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        String inputPath = "./word.txt";
        sparkSession.read()
                .textFile(inputPath)
                .toDF("word")
                .groupBy("word")
                .count()
                .orderBy("count")
                .show();
    }
}
