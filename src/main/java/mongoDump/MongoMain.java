package mongoDump;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * @author Sudev Ambadi
 * 
 */

public class MongoMain {
	
	public static void main(String[] args) {
		
		MongoMain testApp = new MongoMain();
		
		// Spark Context 
		SparkConf conf = new SparkConf().setAppName("CSVs to Mongo");
		conf.setMaster(args[0]);
		// Avoid too much of logging 
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		
		final JavaSparkContext sc = new JavaSparkContext(conf);

		// File path 
		String bsfilepath = "hdfs://<hadoop-namenode>:8020/user/root/phase2/csv/bs.csv";
		String cffilepath = "hdfs://<hadoop-namenode>:8020/user/root/phase2/csv/cf.csv";
		String isfilepath = "hdfs://<hadoop-namenode>:8020/user/root/phase2/csv/is.csv";
		/**
		 * Data CleanUp
		 */
		DataCleaning prep = new DataCleaning();
			
		final Set<String> filterTagsBS = new java.util.HashSet<String>();
		filterTagsBS.add("Total Non Current Assets");
		filterTagsBS.add("Total Assets");
		
		final Set<String> filterTagsCS = new java.util.HashSet<String>();
		filterTagsCS.add("Net cash flows from operating activities");
		filterTagsCS.add("Net cash flows used in investing activities");

		final Set<String> filterTagsIS = new java.util.HashSet<String>();
		filterTagsIS.add("Revenue");
		filterTagsIS.add("Cost of sales");
		
		// Balance sheet
		JavaPairRDD<String, Map<String, Map<String, String>>> bsRdd = DataCleaning
				.dataclean(sc, bsfilepath, filterTagsBS, 4, 0, 2, 1);
		// Income Statement

		JavaPairRDD<String, Map<String, Map<String, String>>> isRdd = DataCleaning
				.dataclean(sc, isfilepath, filterTagsIS, 2, 0, 4, 1);
		// Cash Flow
		JavaPairRDD<String, Map<String, Map<String, String>>> csRdd = DataCleaning
				.dataclean(sc, cffilepath, filterTagsCS, 4, 0, 2, 1);
		
		/*
		 * Union of cleaned dataset
		 */
		JavaPairRDD<String, Map<String, Map<String, String>>> unionRdd = bsRdd
				.union(isRdd).union(csRdd);

		/*
		 * Elegant GroupBy 
		 */
		JavaPairRDD<String, Map<String, Map<String, String>>> reducedRdd = unionRdd
				.reduceByKey(new reduceMaps());

		/**
		 * Mongo Update
		 */
		MongoWrite.writerdd(reducedRdd, "mongodb://10.165.91.54:27017/testrdd.test");
	}

}

final class reduceMaps
		implements
		Function2<Map<String, Map<String, String>>, Map<String, Map<String, String>>, Map<String, Map<String, String>>> {
	public Map<String, Map<String, String>> call(
			Map<String, Map<String, String>> map0,
			Map<String, Map<String, String>> map1) throws Exception {
		Set<Entry<String, Map<String, String>>> emap0 = map0.entrySet();
		// Iterate on map0 and update map1
		for (Entry<String, Map<String, String>> entry : emap0) {
			Map<String, String> val = map1.get(entry.getKey());
			if (val == null) {
				map1.put(entry.getKey(), entry.getValue());
			} else {
				// If present, take union of inner map and replace
				val.putAll(entry.getValue());
				map1.put(entry.getKey(), val);
			}
		}
		return map1;
	}
}
