package edu.miu.cs.cs523;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class Application {

    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {

        // Create a Java Spark Context
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

        // Load our input data
        JavaRDD<String> input = sc.textFile(args[0]);

        // Split up into words
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String x) {
                return Arrays.asList(x.split(" "));
            }
        });

        // Transform into word and count
        JavaPairRDD<String, Integer> counts =
                words.mapToPair(new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String x) {
                        return new Tuple2<String, Integer>(x, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer x, Integer y) {
                        return x + y;
                    }
                });

        // Save the word count back out to a text file, causing evaluation.
        counts.saveAsTextFile(args[1]);

        sc.close();
    }
}
