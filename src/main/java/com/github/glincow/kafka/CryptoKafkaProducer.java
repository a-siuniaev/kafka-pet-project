package com.github.glincow.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.java_websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URI;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CryptoKafkaProducer {

    public KafkaProducer<String, String> producer;

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

    public static void main(String[] args) {
        new CryptoKafkaProducer().run();
    }

    public void run() {
        CryptoCompareWebClient cryptoClient = createCryptoClient();
        cryptoClient.connect();

        while (!cryptoClient.isClosed()) {
            try {
                this.send(cryptoClient.getMsgQueue().poll(5, TimeUnit.SECONDS), "btc-usd");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private CryptoCompareWebClient createCryptoClient()  {
        String apiKey = "ffc08bfd8b5bbe5e45b5809a9722ee6697ed75295ed476eaeb9f68f1c3cefccc";
        String url = "wss://streamer.cryptocompare.com/v2?api_key=" + apiKey;
        CryptoCompareWebClient client = null;
        try {
            client = new CryptoCompareWebClient(new URI(url));
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return client;
    }
}
