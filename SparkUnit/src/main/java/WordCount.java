import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {

    public static List<Tuple2<String, Integer>> countWords(SparkConf sparkConf, String[] args) {

        // create Spark context with Spark configuration
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // read in text file and split each document into words
        JavaRDD<String> tokenized = sc.textFile(args[0]).flatMap(
            new FlatMapFunction<String, String>() {
              public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.toLowerCase().split(" "));
              }
            }
        );

        // count the occurrence of each word
        JavaPairRDD<String, Integer> counts = tokenized.mapToPair(
            new PairFunction<String, String, Integer>() {
              public scala.Tuple2<String, Integer> call(String s) throws Exception {
                return new scala.Tuple2(s, 1);
              }
            }
        ).reduceByKey(
            new Function2<Integer, Integer, Integer>() {
              public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
              }
            }
        );

        return counts.collect();
    }
}