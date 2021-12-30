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

    public static final String BTC_USD_TOPIC = "btc-usd";

    Logger logger = LoggerFactory.getLogger(CryptoKafkaProducer.class);

    public KafkaProducer<String, String> producer;

    public void prepareKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    public void prepareSafeKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        producer = new KafkaProducer<>(properties);
    }

    public void send (String message, String topic) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);

        // send data - async!
        producer.send(record, (metadata, exception) -> {
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
        });
    }

    public static void main(String[] args) {
        new CryptoKafkaProducer().run();
    }

    public void run() {
        CryptoCompareWebClient cryptoClient = createCryptoClient();
        cryptoClient.connect();

        prepareSafeKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            cryptoClient.close();
            producer.close();
            logger.info("app STOPPED");
        }));

        while (!cryptoClient.isClosed()) {
            try {
                this.send(cryptoClient.getMsgQueue().poll(5, TimeUnit.SECONDS), BTC_USD_TOPIC);
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
