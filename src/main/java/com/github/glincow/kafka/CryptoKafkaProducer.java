package com.github.glincow.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CryptoKafkaProducer {

    private KafkaProducer<String, String> producer;

    Logger logger = LoggerFactory.getLogger(CryptoKafkaProducer.class);

    public CryptoKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(properties);
    }

    public void send (String message, String topic) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);

        // send data - async!
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                // executes every time a record is successfully sent or an exception occurs
                if (exception == null) {
                    // the record was succsesfully sent
                    logger.info("Send message to: \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " +  metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp());
                } else {
                    logger.error("Error while producing", exception);
                }
            }
        });
        producer.flush();
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }
}
