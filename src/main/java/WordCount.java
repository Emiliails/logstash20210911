import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        String inputPath = "./word.txt";

        JavaPairRDD<String, Integer> javaPairRDD = sc.textFile(inputPath)
                .flatMap(s -> Arrays.asList(s.split("\\s+")).iterator())
                .mapToPair(word -> new Tuple2<>(word,1))
                .reduceByKey((a,b) -> a+b);

        javaPairRDD.saveAsTextFile("./wordCount");
        System.out.println(javaPairRDD);
    }
}
