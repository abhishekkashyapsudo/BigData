package nagp.kafka;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;

//import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StreamFromKafka {
	private final static String TOPIC = "test-nagp-topic1";

	public static void main(String[] args) throws TimeoutException, StreamingQueryException {

		SparkConf sparkConf = new SparkConf();
		SparkSession spark = SparkSession.builder().master("local[*]").config(sparkConf).appName("streaming-nagp")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> df = spark.readStream().format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", TOPIC).option("startingOffsets", "earliest").load();

		// Generate running word count
		Dataset<String> words = df.selectExpr("CAST(value AS STRING)").as(Encoders.STRING()).flatMap(
				(FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

		// Generate running word count
		Dataset<Row> wordCounts = words.groupBy("value").count().orderBy(new Column("count").desc());
		StreamingQuery query = wordCounts.writeStream().outputMode("complete").format("console").start();

		query.awaitTermination();
	}

}
