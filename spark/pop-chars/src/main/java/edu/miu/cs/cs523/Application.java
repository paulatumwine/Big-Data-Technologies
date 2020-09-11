package edu.miu.cs.cs523;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Application {
    private static final Pattern SPLITTER = Pattern.compile("(?!^)");

    public static void main(String[] args) throws Exception {
        // Create a Java Spark Context
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

        // Load our input data
        JavaRDD<String> lines = sc.textFile(args[0]);
        Integer threshold = Integer.parseInt(args[2]);

        // Calculate word count
        JavaPairRDD<String, Integer> popularWordCounts = lines
                .flatMap(line -> Arrays.asList(line.split(" ")))
                .mapToPair(w -> new Tuple2<String, Integer>(w, 1))
                .reduceByKey((x, y) -> x + y)
                .filter(c -> c._2 > threshold)
                .cache();

        JavaPairRDD<String, Integer> charCounts = popularWordCounts.flatMapToPair(
                c -> Arrays.asList(SPLITTER.split(c._1))
                        .stream()
                        .map(v -> new Tuple2<String, Integer>(v, c._2))
                        .collect(Collectors.toList())
        ).reduceByKey((x, y) -> x + y);

        // Save the word count back out to a text file, causing evaluation
        charCounts.saveAsTextFile(args[1]);

        sc.close();
    }
}

