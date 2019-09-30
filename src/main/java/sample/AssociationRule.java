package sample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.Tuple3;
import shapeless.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AssociationRule
{
	public static void main(String[] args) throws Exception
	{
		List<Tuple2<Tuple2<String,String>,Integer>> doubleItemSet = new ArrayList<>();
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("browsingg.txt");
		JavaRDD<String> products = lines.flatMap(l -> Arrays.asList(l.split("[^\\w]+")).iterator());
		JavaPairRDD<String, Integer> singleItemsetBeforePruning = products.mapToPair(w -> new Tuple2<>(w, 1));
		JavaPairRDD<String, Integer> singleItemsetAfterPruning = singleItemsetBeforePruning.reduceByKey((n1, n2) -> n1 + n2).filter(l -> l._2 < 100);
		JavaPairRDD<String, Integer> singleItemsetAfterPrunings = singleItemsetBeforePruning.reduceByKey((n1, n2) -> n1 + n2).filter(l -> l._2 >= 100);

		JavaRDD<String> singleItemSetFrequentItems = singleItemsetAfterPruning.keys();
		for (int i = 0; i < singleItemsetAfterPrunings.count(); i++)
		{
			for (int j = 0; j < singleItemsetAfterPrunings.count(); j++)
			{
				if (! singleItemsetAfterPrunings.take((int) singleItemsetAfterPrunings.count()).get(i)._1.equals(singleItemsetAfterPrunings.take((int) singleItemsetAfterPrunings.count()).get(j)._1))
				{
					doubleItemSet.add(new Tuple2<>(new Tuple2<>(singleItemsetAfterPrunings.take((int) singleItemsetAfterPrunings.count()).get(i)._1, singleItemsetAfterPrunings.take((int) singleItemsetAfterPrunings.count()).get(j)._1),singleItemsetAfterPrunings.take((int) singleItemsetAfterPrunings.count()).get(i)._2 *singleItemsetAfterPrunings.take((int) singleItemsetAfterPrunings.count()).get(j)._2));
				}
			}
		}

		//JavaPairRDD<String, String> doubleItemSet = singleItemSetFrequentItems.cartesian(singleItemSetFrequentItems).filter(l -> !l._2.equals(l._1));
		JavaPairRDD<Tuple2<String, String>, Integer> doubleItemSetFrequentItems = sc.parallelizePairs(doubleItemSet);
		//JavaPairRDD<Tuple2<String, String>, Integer> doubleItemSetPairs = doubleItemSetFrequentItems.mapToPair(w -> new Tuple2<>(new Tuple2<>(w._1, w._2), 1));
		JavaPairRDD<Tuple2<String, String>, Integer> doubleItemSetCount = doubleItemSetFrequentItems.reduceByKey(Integer::sum);
		singleItemsetAfterPrunings.saveAsTextFile("Outputs.txt");
		doubleItemSetFrequentItems.saveAsTextFile("Outputss.txt");
		doubleItemSetCount.saveAsTextFile("Output.txt");

		sc.stop();
	}
}
