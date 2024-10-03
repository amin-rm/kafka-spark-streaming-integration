package binod.suman.kafka;



import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

public class CSVKafkaProducer {

    // Updated to use call_logs
    private static String KafkaBrokerEndpoint = "192.168.59.184:9092";
    private static String KafkaTopic = "call-logs";
    private static String CsvFile = "test.csv";

    private Producer<String, String> ProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCsvProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    public static void main(String[] args) throws URISyntaxException {
        CSVKafkaProducer kafkaProducer = new CSVKafkaProducer();
        kafkaProducer.PublishMessages();
        System.out.println("Producing job completed");
    }

    private void PublishMessages() throws URISyntaxException {
        final Producer<String, String> csvProducer = ProducerProperties();

        try {
            URI uri = getClass().getClassLoader().getResource(CsvFile).toURI();
            Stream<String> FileStream = Files.lines(Paths.get(uri));

            // Read each line, skip header, and process each record
            FileStream.skip(1).forEach(line -> {
                // Split CSV line to extract fields
                String[] attributes = line.split(",");

                // Create structured JSON-like string
                String callLogJson = String.format(
                        "{ \"customerID\": \"%s\", \"Location\": \"%s\", \"Date\": \"%s\", \"callDuration\": \"%s\", \"callType\": \"%s\" }",
                        attributes[0], attributes[1], attributes[2], attributes[3], attributes[4]);

                // Create a new ProducerRecord with the structured message
                final ProducerRecord<String, String> csvRecord = new ProducerRecord<>(
                        KafkaTopic, UUID.randomUUID().toString(), callLogJson);

                // Send the message to Kafka
                csvProducer.send(csvRecord, (metadata, exception) -> {
                    if (metadata != null) {
                        System.out.println("Sent: " + csvRecord.key() + " | " + csvRecord.value());
                    } else {
                        System.out.println("Error Sending Csv Record -> " + csvRecord.value());
                    }
                });
            });

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            csvProducer.close();
        }
    }
}

