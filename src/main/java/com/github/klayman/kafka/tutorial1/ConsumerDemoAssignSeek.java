package com.github.klayman.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";


        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are mostly used to replay data or fetch a specific message
        TopicPartition topicPartitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(topicPartitionToReadFrom));

        //seek
        long offsetToReadFrom = 15L;
        consumer.seek(topicPartitionToReadFrom, 15);

        //poll for new data
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesRead = 0;

        while (keepOnReading) {
            //consumer.poll(100) // deprecated starting from Kafka 2.0.0
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesRead += 1;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                if (numberOfMessagesRead >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting the app...");
    }
}
