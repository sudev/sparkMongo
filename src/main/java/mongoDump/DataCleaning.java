package mongoDump;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

/**
 * @author Sudev Ambadi
 * 
 */
public class DataCleaning {
	
	public static JavaPairRDD<String, Map<String, Map<String, String>>> prepros(
			JavaSparkContext sc, String filepath, final Set<String> filterTag,
			final int pos_tag,  final int pos_cname,
			final int pos_date, final int pos_value) {
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame df = sqlContext.read().format("com.databricks.spark.csv")
				.option("header", "true").load(filepath);
		JavaRDD<Row> rowrdd = df.javaRDD();
		JavaPairRDD<String, Map<String, Map<String, String>>> mapedrdd = rowrdd
				.filter(new Function<Row, Boolean>() {
					@Override
					public Boolean call(Row r) throws Exception {
						return filterTag.contains(r.getString(pos_tag));
					}
				})
				.mapToPair(
						new PairFunction<Row, String, Map<String, Map<String, String>>>() {
							@Override
							public Tuple2<String, Map<String, Map<String, String>>> call(
									Row r) {
								Map<String, String> m1 = new HashMap<String, String>();
								Map<String, Map<String, String>> m2 = new HashMap<String, Map<String, String>>();
								String label = r.getString(pos_tag);
								// create a map of the form { Tag : value }
								m1.put(label, r.getString(pos_value));
								String year = r.getString(pos_date).substring(
										r.getString(pos_date).length() - 4);
								// create a map of the form 
								// { year :  { tag : value }   }
								m2.put(year, m1);
								return new Tuple2<String, Map<String, Map<String, String>>>(r.getString(pos_cname), m2);
							}
						});
		return mapedrdd;
	}

}
