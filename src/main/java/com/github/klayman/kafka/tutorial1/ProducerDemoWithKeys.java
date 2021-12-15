package com.github.klayman.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        // create Producer properties
        Properties properties = new Properties();

        //hard-coded way to create properties
        //properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            // this is how kafka now how to serialize messages to bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create Producer
            // producer with kay and value as String, String
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=1; i<=100; i++) {
            // create a producer record
            String topic = "first_topic";
            String value = "hello world " + i;
            String key = "id_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key);

            // send data - async!
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes every time a record is successfully sent or an exception occurs
                    if (exception == null) {
                        // the record was succsesfully sent
                        logger.info("Recieved new metadata: \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " +  metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            }).get(); // blocking send method to make it synchronous - DONT DO THIS IN PRODUCTION!
        }

        producer.flush();
        producer.close();
    }
}
