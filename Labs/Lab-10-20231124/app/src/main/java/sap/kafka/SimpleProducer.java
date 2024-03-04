package sap.kafka;

import java.util.Properties;
import java.time.Duration;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SimpleConsumer {
    public static void main(String[] args) throws Exception {

        // Define the name of the Kafka topic we want to consume from
        String topicName = "my-event-channel";

        // Create a Properties object to hold the configuration settings for the Kafka
        // Consumer
        Properties props = new Properties();

        // Specify the address of the Kafka brokers
        props.put("bootstrap.servers", "localhost:29092");

        // Specify the consumer group this consumer will belong to
        props.put("group.id", "test");

        // Enable automatic commit of offsets
        props.put("enable.auto.commit", "true");

        // Specify the frequency of auto-commit in milliseconds
        props.put("auto.commit.interval.ms", "1000");

        // Specify the session timeout in milliseconds
        props.put("session.timeout.ms", "30000");

        // Specify the deserializer class for keys
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Specify the deserializer class for values
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Create a KafkaConsumer instance and subscribe to the specified topic
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // Subscribe to the specified topic
        consumer.subscribe(Arrays.asList(topicName));

        // Print the name of the topic we're subscribed to
        System.out.println("Subscribed to topic " + topicName);

        // Continuously poll for new data records from the Kafka topic
        while (true) {
            // Poll the Kafka server for data, waiting indefinitely if none arrives
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));

            // Placeholder for application-specific processing of the data
            // process(records);

            // Iterate over each record from the batch of records fetched from Kafka
            for (ConsumerRecord<String, String> record : records)

                // Print the offset, key, and value for each record
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
            // Synchronously commit the current offset of this consumer
            consumer.commitSync();
        }
    }
}