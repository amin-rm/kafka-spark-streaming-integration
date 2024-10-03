package binod.suman.spark;

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.RowFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class SparkBatchProcessorTest extends JavaDatasetSuiteBase {

    private SparkSession spark;

    @Before
    public void setUp() {
        // Initialize a SparkSession for testing
        spark = super.spark();
    }


    @Test
    public void testDateParsing() {
        // Prepare test data
        List<Row> data = Arrays.asList(
                RowFactory.create("1", "2023-10-04 12:30:00", "LocationA"),
                RowFactory.create("2", "2023-10-04 12:45:00", "LocationB")
        );

        // Define schema for the test data
        StructType schema = new StructType(new StructField[]{
                new StructField("customerID", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Date", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Location", DataTypes.StringType, false, Metadata.empty())
        });

        // Create a test DataFrame
        Dataset<Row> testDF = spark.createDataFrame(data, schema);

        // Parse the Date field into a timestamp column
        Dataset<Row> parsedCallData = testDF.withColumn("callTime",
                functions.to_timestamp(testDF.col("Date"), "yyyy-MM-dd HH:mm:ss"));

        // Assert that the new column 'callTime' is not null
        assertEquals("Should have a non-null callTime column",
                parsedCallData.filter(parsedCallData.col("callTime").isNotNull()).count(), 2);
    }

    @Test
    public void testWindowAggregation() {
        // Prepare test data with overlapping times
        List<Row> data = Arrays.asList(
                RowFactory.create("1", "2023-10-04 12:30:00", "LocationA"),
                RowFactory.create("1", "2023-10-04 12:31:00", "LocationB"),
                RowFactory.create("1", "2023-10-04 12:32:00", "LocationC"),
                RowFactory.create("2", "2023-10-04 12:45:00", "LocationD")
        );

        // Define schema for the test data
        StructType schema = new StructType(new StructField[]{
                new StructField("customerID", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Date", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Location", DataTypes.StringType, false, Metadata.empty())
        });

        // Create a test DataFrame
        Dataset<Row> testDF = spark.createDataFrame(data, schema);

        // Parse the Date field into a timestamp column
        Dataset<Row> parsedCallData = testDF.withColumn("callTime",
                functions.to_timestamp(testDF.col("Date"), "yyyy-MM-dd HH:mm:ss"));

        // Define a 30-minute sliding window for each customerID, grouped by Location
        Dataset<Row> stateChanges = parsedCallData
                .groupBy(
                        parsedCallData.col("customerID"),
                        functions.window(parsedCallData.col("callTime"), "30 minutes"),
                        parsedCallData.col("Location")
                )
                .count();

        // Verify that state changes were aggregated correctly
        assertEquals("Should have 4 rows after grouping by window and location", 4, stateChanges.count());
    }

    @Test
    public void testSimCloningDetection() {
        // Prepare test data to simulate SIM cloning
        List<Row> data = Arrays.asList(
                RowFactory.create("1", "2023-10-04 12:30:00", "LocationA"),
                RowFactory.create("1", "2023-10-04 12:35:00", "LocationB"), // Same customer in different location
                RowFactory.create("2", "2023-10-04 12:45:00", "LocationC"),
                RowFactory.create("2", "2023-10-04 12:50:00", "LocationC")
        );

        // Define schema for the test data
        StructType schema = new StructType(new StructField[]{
                new StructField("customerID", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Date", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Location", DataTypes.StringType, false, Metadata.empty())
        });

        // Create a test DataFrame
        Dataset<Row> testDF = spark.createDataFrame(data, schema);

        // Parse the Date field into a timestamp column
        Dataset<Row> parsedCallData = testDF.withColumn("callTime",
                functions.to_timestamp(testDF.col("Date"), "yyyy-MM-dd HH:mm:ss"));

        // Detect potential SIM cloning
        Dataset<Row> stateChanges = parsedCallData
                .groupBy(
                        parsedCallData.col("customerID"),
                        functions.window(parsedCallData.col("callTime"), "30 minutes"),
                        parsedCallData.col("Location")
                )
                .count();

        Dataset<Row> clonedSIMs = stateChanges
                .groupBy("customerID", "window")
                .agg(functions.collect_set("Location").as("Locations"))
                .where(functions.size(functions.col("Locations")).gt(1)) // Same customer, different locations
                .select("customerID", "Locations", "window");

        // Verify that a SIM cloning instance was detected
        assertEquals("Should detect 1 SIM cloning instance", 1, clonedSIMs.count());
    }
}
