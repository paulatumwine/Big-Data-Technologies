package edu.miu.cs.cs523;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Application
{

    public static void main(String[] args) throws Exception
    {
        // Create a Java Spark Context
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

        // Load our input data
        JavaRDD<String> lines = sc.textFile(args[0]);

        // Calculate word count
        JavaPairRDD<String, Integer> counts = lines
                .flatMap(line -> Arrays.asList(line.split(" ")))
                .mapToPair(w -> new Tuple2<String, Integer>(w, 1))
                .reduceByKey((x, y) -> x + y);

        // Save the word count back out to a text file, causing evaluation
        counts.saveAsTextFile(args[1]);

        sc.close();
    }
}

