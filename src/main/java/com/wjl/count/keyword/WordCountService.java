package com.wjl.count.keyword;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * spark 统计 demo
 */
@Component
public class WordCountService implements Serializable, CommandLineRunner {

    @Autowired
    private transient JavaSparkContext sc;

    @Override
    public void run(String... args) {
        // 文件可以是本地文件系统的文件, 也可以是 hdfs 文件系统的文件
        JavaRDD<String> lines = sc.textFile("/Users/duandongdong/Downloads/测试文件.txt").cache();
        countStr("关键词", lines);
        wordFrequency(lines);
    }

    /**
     * 统计关键词出现次数
     * @param keyWord 关键词
     * @param lines 行 RDD
     */
    public void countStr(String keyWord, JavaRDD<String> lines) {
        lines.map((a) -> a);
        JavaRDD<Integer> spu = lines.map(i -> StringUtils.countOccurrencesOf(i.toString(), keyWord));
        int c = spu.reduce((a, b) -> a + b);
    }

    /**
     * 统计词频, 输出到文件
     * @param lines 行 RDD
     */
    public void wordFrequency(JavaRDD<String> lines) {

        JavaRDD<String> splitRDD = lines.flatMap((FlatMapFunction<String, String>) s ->
                Arrays.asList(s.split(" ")).iterator());

        JavaPairRDD<String, Integer> splitFlagRDD = splitRDD.mapToPair((PairFunction<String, String, Integer>) s ->
                new Tuple2<>(s,1));

        JavaPairRDD<String, Integer> countRDD = splitFlagRDD.reduceByKey((Function2<Integer, Integer, Integer>)
                (integer, integer2) -> integer+integer2);

        countRDD.saveAsTextFile("./result");
    }
}
