package sap.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumer {
   public static void main(String[] args) throws Exception {

      // Kafka consumer configuration settings
      String topicName = "my-event-channel";
      // Create a Properties object to store the configuration settings for the Kafka
      // Consumer.
      // The Properties class represents a persistent set of properties, which are
      // key-value pairs.
      Properties props = new Properties();

      // Specify the address of the Kafka brokers. This is where the Kafka Consumer
      // will connect to.
      // The value is a list of host/port pairs to use for establishing the initial
      // connection to the Kafka cluster.
      props.put("bootstrap.servers", "localhost:29092");

      // Specify the consumer group this consumer will belong to.
      // A consumer group includes the set of consumer processes that are subscribing
      // to a specific topic.
      props.put("group.id", "test");

      // Enable automatic commit of offsets. This means that the consumer's offset
      // will be periodically committed in the background.
      props.put("enable.auto.commit", "true");

      // Specify the frequency of auto-commit in milliseconds. This is how often the
      // consumer's offset will be committed.
      props.put("auto.commit.interval.ms", "1000");

      // Specify the session timeout in milliseconds. If the consumer stops sending
      // heartbeats for this amount of time, the consumer will be considered dead and
      // its partitions will be re-assigned.
      props.put("session.timeout.ms", "30000");

      // Specify the deserializer class for keys. This class will be used to
      // deserialize the key of each record.
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

      // Specify the deserializer class for values. This class will be used to
      // deserialize the value of each record.
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

      // Create a KafkaConsumer instance with the specified properties.
      // The KafkaConsumer reads records from one or more Kafka topics.
      KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

      // The Kafka Consumer subscribes to a list of topics here.
      // The Arrays.asList method returns a fixed-size list backed by the specified
      // array.
      consumer.subscribe(Arrays.asList(topicName));

      // Print the name of the topic to which we're subscribed.
      System.out.println("Subscribed to topic " + topicName);

      // Continuously poll for new data records from the Kafka topic.
      while (true) {
         // Poll the Kafka server for data, waiting indefinitely if none arrives.
         // The Duration.ofMillis method creates a duration of the specified number of
         // milliseconds.
         ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));

         // Placeholder for application-specific processing of the data.
         // process(records);

         // Iterate over each record from the batch of records fetched from Kafka.
         for (ConsumerRecord<String, String> record : records)

            // Print the offset, key, and value for each record.
            // The offset is a unique identifier of a record within a partition. It denotes
            // the position of the consumer in the partition.
            System.out.printf("offset = %d, key = %s, value = %s\n",
                  record.offset(), record.key(), record.value());
         // Synchronously commit the current offset of this consumer.
         // This means that the consumer will not proceed until its offset has been
         // committed.
         consumer.commitSync();
      }
   }
}