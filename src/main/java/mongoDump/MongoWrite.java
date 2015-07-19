package mongoDump;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.MongoOutputFormat;

import scala.Tuple2;

/**
 * @author Sudev Ambadi
 * 
 */

public class MongoWrite {

	public static void writerdd(JavaPairRDD<String, Map<String, Map<String, String>>> inputRdd, String mongouri) {
		// Mongo-Hadoop Specific configuration
		org.apache.hadoop.conf.Configuration midbconf = new org.apache.hadoop.conf.Configuration();
		midbconf.set("mongo.output.format",
				"com.mongodb.hadoop.MongoOutputFormat");
		midbconf.set("mongo.output.uri", mongouri);

		// Map RDD to MongoDB Objects
		JavaPairRDD<Object, BSONObject> mongordd = inputRdd
				.mapToPair(new basicDBMongo());
		// Update MongoDB
		mongordd.saveAsNewAPIHadoopFile("file:///notapplicable", Object.class,
				Object.class, MongoOutputFormat.class, midbconf);
	}
}

final class basicDBMongo implements PairFunction<Tuple2<String, Map<String, Map<String, String>>>, Object, BSONObject> {
	public Tuple2<Object, BSONObject> call(
			Tuple2<String, Map<String, Map<String, String>>> companyTuple)
			throws Exception {
		BasicBSONObject report = new BasicBSONObject();
		// Create a BSON of form { companyName : financeDetails } 
		report.put(companyTuple._1(), companyTuple._2());
		return new Tuple2<Object, BSONObject>(null, report);
	}
}