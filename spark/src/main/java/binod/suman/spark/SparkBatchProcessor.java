package binod.suman.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SparkBatchProcessor {

    public static void main(String[] args) {
        // Initialize a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("SimCloningDetectorBatch")
                .master("local[*]") // For local testing, use all available cores
                .getOrCreate();

        // Path to the CSV file
        String csvFilePath = "/mnt/spark/work/test.csv"; // Hardcoded CSV file path

        // Read the CSV file into a DataFrame
        Dataset<Row> callData = spark.read()
                .option("header", "true") // Use the first line as header
                .option("inferSchema", "true") // Automatically infer data types
                .csv(csvFilePath);

        // Show the data read from the CSV file (for debugging)
        callData.show();

        // Parse the Date field into a timestamp column
        Dataset<Row> parsedCallData = callData
                .withColumn("callTime", functions.to_timestamp(callData.col("Date"), "yyyy-MM-dd HH:mm:ss"));

        // Define a 30-minute sliding window for each customerID, grouped by Location
        Dataset<Row> stateChanges = parsedCallData
                .groupBy(
                        parsedCallData.col("customerID"),
                        functions.window(parsedCallData.col("callTime"), "30 minutes"),
                        parsedCallData.col("Location")
                )
                .count();

        // Detect potential SIM cloning: Same customerID, different locations within the window
        Dataset<Row> clonedSIMs = stateChanges
                .groupBy("customerID", "window")
                .agg(functions.collect_set("Location").as("Locations"))
                .where(functions.size(functions.col("Locations")).gt(1)) // Check if the same customer is in more than one location
                .select("customerID", "Locations", "window");

        // Show the results in the Spark logs
        clonedSIMs.show(100, false); // Display up to 100 rows without truncating

        // Stop the Spark session
        spark.stop();
    }
}
