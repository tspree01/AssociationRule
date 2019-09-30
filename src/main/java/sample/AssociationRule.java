package sample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import shapeless.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AssociationRule
{
	public static void main(String[] args) throws Exception
	{
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("browsingg.txt");
		JavaRDD<String> products = lines.flatMap(l -> Arrays.asList(l.split("[^\\w]+")).iterator());
		JavaPairRDD<String, Integer> singleItemsetBeforePruning = products.mapToPair(w -> new Tuple2<>(w, 1));
		JavaPairRDD<String, Integer> singleItemsetAfterPruning = singleItemsetBeforePruning.reduceByKey((n1, n2) -> n1 + n2).filter(l -> l._2 < 100);
		JavaPairRDD<String, Integer> singleItemsetAfterPrunings = singleItemsetBeforePruning.reduceByKey((n1, n2) -> n1 + n2).filter(l -> l._2 >= 100);

		//JavaRDD<String> singleItemSetFrequentItems = products.subtract(singleItemsetAfterPruning.keys());

		//JavaPairRDD<String, String> doubleItemSet = singleItemSetFrequentItems.cartesian(singleItemSetFrequentItems).filter(l -> !l._2.equals(l._1));

		JavaPairRDD<Tuple2<String, String>, Integer> doubleItemSetPairs = singleItemsetAfterPrunings.mapToPair(w -> new Tuple2<>(new Tuple2<>(w._1, w._1), w._2 * w._2));
		JavaPairRDD<Tuple2<String, String>, Integer> doubleItemSetCount = doubleItemSetPairs.reduceByKey(Integer::sum);
		singleItemsetAfterPrunings.saveAsTextFile("Outputs.txt");
		doubleItemSetCount.saveAsTextFile("Output.txt");

		sc.stop();
	}
}
