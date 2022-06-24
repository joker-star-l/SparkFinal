package me.lmx.wordcount;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;

public class Calculation {

    public static JavaPairRDD<String, Integer> groupByKey(JavaRDD<String> lines) {
        JavaPairRDD<String, Integer> words = lines
                .flatMap(o -> Arrays.asList(o.split(" ")).iterator())
                .mapToPair(o -> new Tuple2<>(o, 1));

        JavaPairRDD<String, Integer> counts = words
                .groupByKey()
                .mapToPair(o -> {
                    int sum = 0;
                    for (int i : o._2) {
                        sum += i;
                    }
                    return new Tuple2<>(o._1, sum);
                });

        return counts;
    }

    public static JavaPairRDD<String, Integer> reduceByKey(JavaRDD<String> lines) {
        JavaPairRDD<String, Integer> words = lines
                .flatMap(o -> Arrays.asList(o.split(" ")).iterator())
                .mapToPair(o -> new Tuple2<>(o, 1));

        JavaPairRDD<String, Integer> counts = words.reduceByKey(Integer::sum);

        return counts;
    }
}
