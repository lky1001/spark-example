package com.tistory.lky1001.sparkexample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by soundllydev on 2016. 10. 24..
 */
public class SparkExample {

    public static void main(String[] args) {
        SparkExample example = new SparkExample();
        example.boot();
    }

    public void boot() {
        SparkConf conf = new SparkConf().setAppName("example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);

        /**
         * reduce 결과 값 : 바로 전 단계의 리턴 값이 이번 단계의 첫 번째 인자로 들어와 누적되게 된다.
         * a : 1, b : 2
         * a : 3, b : 3
         * a : 6, b : 4
         * a : 10, b : 5
         */
        int total = distData.reduce((a, b) -> {
            System.out.printf("a : %d, b : %d\n", a, b);
            return a + b;
        });

        System.out.printf("\n\n\ntotal : %d\n", total);

        // 파일 글자수 세기
        JavaRDD<String> lines = sc.textFile("test.txt");

        // 파일에서 줄 단위로 읽어서 글자 길이를 담는다.
        JavaRDD<Integer> lineLengths = lines.map(s -> {
            System.out.printf("s : %s\n", s);
            return s.length();
        });

        // 줄 단위로 읽은 글자 길이를 누적한다.
        int totalLength = lineLengths.reduce((a, b) -> a + b);

        System.out.printf("\n\n\ntotalLength : %d\n", totalLength);
    }
}
