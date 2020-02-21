package sample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.columnar.MAP;
import scala.Tuple2;
import scala.Tuple3;
import scala.math.Ordering;
import shapeless.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AssociationRule
{
	public static void main(String[] args) throws Exception
	{
		List<String> baskets = new ArrayList<>();
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		final int support = 100;
		JavaRDD<String> lines = sc.textFile("browsingg.txt");
		JavaRDD<String> products = lines.flatMap(l -> Arrays.asList(l.split(" ")).iterator());
		JavaPairRDD<String, Integer> singleItemsetBeforePruning = products.mapToPair(w -> new Tuple2<>(w, 1));
		JavaPairRDD<String, Integer> singleItemSetAfterPruning = singleItemsetBeforePruning.reduceByKey((n1,n2) -> n1 + n2).filter(l -> l._2 >= support);
		List<Tuple2<String, Integer>> listOfSingleItemProducts = singleItemSetAfterPruning.collect();


		//singleItemSetAfterPruning.saveAsTextFile("Outputssss.txt");

		String fileName = "browsingg.txt";
		File InputFile = new File(fileName);
		FileInputStream InputFileStream = new FileInputStream(InputFile);
		InputStreamReader isr = new InputStreamReader(InputFileStream);
		BufferedReader br = new BufferedReader(isr);
		String line;
		while ((line = br.readLine()) != null)
		{
			//process the line
			baskets.add(line);
		}
		br.close();

		List<Tuple2<String, String>> doubleItemSet = new ArrayList<>();
		List<Tuple2<Tuple2<String, String>, Integer>> doubleItemSetCount = new ArrayList<>();
		List<Tuple2<Tuple2<String, String>, Double>> doubleItemSetConfidenceScore = new ArrayList<>();

		for (int i = 0; i < listOfSingleItemProducts.size(); i++)
		{
			for (int j = 0; j < listOfSingleItemProducts.size(); j++)
			{
				if (! listOfSingleItemProducts.get(i).equals(listOfSingleItemProducts.get(j)))
				{
					doubleItemSet.add(new Tuple2<>(listOfSingleItemProducts.get(i)._1, listOfSingleItemProducts.get(j)._1));
				}
			}
		}
		for (int i = 0; i < doubleItemSet.size(); i++)
		{
			for (int j = 0; j < doubleItemSet.size(); j++)
			{
					if(doubleItemSet.get(i).equals(doubleItemSet.get(j).swap()))
					{
						doubleItemSet.remove(j);
					}
			}
		}

		for (String basket : baskets)
		{
			for (int j = 0; j < doubleItemSet.size(); j++)
			{
				if (basket.contains(doubleItemSet.get(j)._1) && basket.contains(doubleItemSet.get(j)._2))
				{
					doubleItemSetCount.add(new Tuple2<>(doubleItemSet.get(j), 1));
				}
			}
		}

		JavaPairRDD<Tuple2<String, String>, Integer> doubleItemSetFrequentItems = sc.parallelizePairs(doubleItemSetCount);
		JavaPairRDD<Tuple2<String, String>, Integer> doubleItemSetAfterPruning = doubleItemSetFrequentItems.reduceByKey(Integer::sum).filter(l -> l._2 >= support);
		List<Tuple2<Tuple2<String, String>, Integer>> listOfDoubleItemProducts = doubleItemSetAfterPruning.collect();

		for (int i = 0; i < listOfDoubleItemProducts.size(); i++)
		{
			double confidenceScore = (double)listOfDoubleItemProducts.get(i)._2/(double)singleItemSetAfterPruning.lookup(listOfDoubleItemProducts.get(i)._1._1).get(0);
			doubleItemSetConfidenceScore.add(new Tuple2<>(listOfDoubleItemProducts.get(i)._1,confidenceScore));
		}
		JavaPairRDD<Tuple2<String, String>, Double> doubleItemSetFrequentItemsCofidenceScores = sc.parallelizePairs(doubleItemSetConfidenceScore);
		doubleItemSetFrequentItemsCofidenceScores.saveAsTextFile("DoubleItemSetConfidenceScores.txt");

		List<Tuple3<String, String, String>> tripleItemSet = new ArrayList<>();
		List<Tuple2<Tuple3<String, String, String>, Integer>> tripleItemSetCount = new ArrayList<>();
		for (int i = 0; i < listOfDoubleItemProducts.size(); i++)
		{
			for (int j = 0; j < listOfDoubleItemProducts.size(); j++)
			{
				if (! listOfDoubleItemProducts.get(i)._1._1.equals(listOfDoubleItemProducts.get(j)._1._2) && ! listOfDoubleItemProducts.get(i)._1._2.equals(listOfDoubleItemProducts.get(j)._1._2) )
				{
					tripleItemSet.add(new Tuple3<>(listOfDoubleItemProducts.get(i)._1._1, listOfDoubleItemProducts.get(i)._1._2, listOfDoubleItemProducts.get(j)._1._2));
				}
			}
		}
		JavaRDD<Tuple3<String, String, String>> distinctTripleItemSetFrequentItems = sc.parallelize(tripleItemSet).distinct();
		List<Tuple3<String, String, String>> distinctTripleItemSet = distinctTripleItemSetFrequentItems.collect();
		List<Tuple2<Tuple3<String, String, String>, Double>> tripleItemSetConfidenceScore = new ArrayList<>();


		distinctTripleItemSetFrequentItems.saveAsTextFile("Outputssss.txt");

		for (int i = 0; i < baskets.size(); i++)
		{
			for (int j = 0; j < distinctTripleItemSet.size(); j++)
			{
				if (baskets.get(i).contains(distinctTripleItemSet.get(j)._1()) && baskets.get(i).contains(distinctTripleItemSet.get(j)._2()) && baskets.get(i).contains(distinctTripleItemSet.get(j)._3()))
				{
					tripleItemSetCount.add(new Tuple2<>(tripleItemSet.get(j), 1));
				}
			}
		}

		JavaPairRDD<Tuple3<String, String, String>, Integer> tripleItemSetFrequentItems = sc.parallelizePairs(tripleItemSetCount);
		JavaPairRDD<Tuple3<String, String, String>, Integer> tripleItemSetAfterPruning = tripleItemSetFrequentItems.reduceByKey(Integer::sum).filter(l -> l._2 >= support);
		List<Tuple2<Tuple3<String, String, String>, Integer>> listOfTripleItemProducts = tripleItemSetAfterPruning.collect();
		for (int i = 0; i < listOfTripleItemProducts.size(); i++)
		{
			int test = singleItemSetAfterPruning.lookup(listOfTripleItemProducts.get(i)._1._1()).get(0);
			int test2 = listOfTripleItemProducts.get(i)._2;
			double confidenceScore = (double)listOfTripleItemProducts.get(i)._2/(double)singleItemSetAfterPruning.lookup(listOfTripleItemProducts.get(i)._1._1()).get(0);
			tripleItemSetConfidenceScore.add(new Tuple2<>(listOfTripleItemProducts.get(i)._1,confidenceScore));
		}
		JavaPairRDD<Tuple3<String, String, String>, Double> tripleItemSetFrequentItemsConfidenceScores = sc.parallelizePairs(tripleItemSetConfidenceScore);

		tripleItemSetFrequentItemsConfidenceScores.saveAsTextFile("Outputs.txt");
		sc.stop();
	}
}