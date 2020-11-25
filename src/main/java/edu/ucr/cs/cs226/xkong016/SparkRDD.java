package edu.ucr.cs.cs226.xkong016;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class SparkRDD
{
    public static void main (String[] args ) {
        if(args.length < 1){
            System.out.println("ERROR : Please enter the path of input file!");
            System.out.println("EXITING");
            System.exit(1);
        }
        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filename = args[0];
        JavaRDD<String> lines = sc.textFile(filename);

        // Grouped aggregation
        JavaPairRDD<Integer, Double> RespBytes = lines
                .map(line -> line.split("\t"))
                .mapToPair(line -> new Tuple2<>(Integer.parseInt(line[5]),Double.parseDouble(line[6])));

        JavaPairRDD<Integer, Double> DeducedPairs = RespBytes
                .reduceByKey((a,b) -> a+b);

        Map<Integer, Long> KeyCount = RespBytes
                .countByKey();

        JavaRDD<String> Avg = DeducedPairs
                .map(rp -> "Code: "+rp._1+", average number of bytes = "+rp._2/KeyCount.get(rp._1));
        Avg.saveAsTextFile("task1.txt");

        // Self Join
        JavaPairRDD<String, String[]> Pairs = lines
                .map(line -> line.split("\t"))
                .mapToPair(line -> new Tuple2<>(line[0]+line[4],line));

        JavaRDD<Tuple2<String[], String[]>> JoinedPairs = Pairs
                .join(Pairs)
                .values()
                .filter(line -> !Arrays.equals(line._1, line._2) &&
                        (Math.abs(Long.parseLong(line._1[2]) - Long.parseLong(line._2[2])) <=3600 ));

        JavaRDD<String> Joined = JoinedPairs
                .map(jp -> String.join("\t",jp._1)+"\t"+String.join("\t",jp._2));
        Joined.saveAsTextFile("task2.txt");

        sc.stop();
        sc.close();
    }

}
