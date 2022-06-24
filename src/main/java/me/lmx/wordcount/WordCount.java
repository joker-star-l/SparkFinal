package me.lmx.wordcount;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;

public class WordCount {

    public static void run(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
//        conf.setMaster("local"); // 本地运行，若在集群中运行需要删除

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]);
//        JavaRDD<String> lines = sc.textFile(args[0], 2);

        System.out.println(lines.getNumPartitions()); // 打印分区数

        JavaPairRDD<String, Integer> wordCounts = Calculation.groupByKey(lines); // groupByKey
//        JavaPairRDD<String, Integer> wordCounts = Calculation.reduceByKey(lines); // reduceByKey

        FileUtils.deleteQuietly(new File(args[1]));
        wordCounts.saveAsTextFile(args[1]);
        sc.stop();
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        run(args);

        long end = System.currentTimeMillis();

        System.out.println("总运行时间：" + (end - start) + "ms");
    }
}
