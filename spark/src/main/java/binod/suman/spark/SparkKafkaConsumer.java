package binod.suman.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

public class SparkKafkaConsumer {

	public static void main(String[] args) throws StreamingQueryException, TimeoutException {

		// Initialize a SparkSession
		SparkSession spark = SparkSession.builder()
				.appName("SimCloningDetector")
				.master("local[*]")
				.getOrCreate();

		// Set the configuration to bypass stateful correctness check (use with caution)
		spark.conf().set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false");


		// Read from Kafka topic using Structured Streaming
		Dataset<Row> kafkaStream = spark
				.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", "10.104.209.170:9092")
				.option("subscribe", "call-logs")
				.option("startingOffsets", "earliest")
				.load();

		// Deserialize the Kafka value into String format
		Dataset<Row> callLogs = kafkaStream.selectExpr("CAST(value AS STRING)");

		// Define the schema of the call logs (as per the producer)
		Dataset<Row> callData = callLogs
				.select(functions.from_json(callLogs.col("value"),
								functions.schema_of_json("{\"customerID\":\"string\",\"Location\":\"string\",\"Date\":\"string\",\"callDuration\":\"string\",\"callType\":\"string\"}"))
						.as("data"))
				.select("data.*");

		// Parse the Date field into a timestamp column and add a watermark for 30 minutes
		Dataset<Row> parsedCallData = callData
				.withColumn("callTime", functions.to_timestamp(callData.col("Date"), "yyyy-MM-dd HH:mm:ss"))
				.withWatermark("callTime", "30 minutes");

		// Define a 30-minute sliding window for each customerID, grouped by Location
		Dataset<Row> stateChanges = parsedCallData
				.groupBy(
						parsedCallData.col("customerID"),
						functions.window(parsedCallData.col("callTime"), "30 minutes"),  // 30-minute window
						parsedCallData.col("Location")
				)
				.count();

		// Detect potential SIM cloning: Same customerID, different states within the window
		Dataset<Row> clonedSIMs = stateChanges
				.groupBy("customerID", "window")
				.agg(functions.collect_set("Location").as("Locations"))
				.where(functions.size(functions.col("Locations")).gt(1))  // Check if the same customer is in more than one location
				.select("customerID", "Locations", "window");

		// Output the alerts to the console and to Spark UI
		StreamingQuery query = clonedSIMs
				.writeStream()
				.outputMode("update")
				.format("console")  // Print the alerts to the console
				.option("truncate", "false")
				.trigger(Trigger.ProcessingTime("1 minute"))  // Set the trigger interval
				.start();

		// Keep the streaming query running
		query.awaitTermination();
	}
}
