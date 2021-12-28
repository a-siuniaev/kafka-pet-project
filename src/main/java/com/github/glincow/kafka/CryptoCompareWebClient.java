package com.github.glincow.kafka;


import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example demonstrates how to create a websocket connection to a server. Only the most
 * important callbacks are overloaded.
 */
public class CryptoCompareWebClient extends WebSocketClient {

    private static final String BTC_USD_TOPIC = "btc-usd";

    Logger logger = LoggerFactory.getLogger(CryptoCompareWebClient.class);

    public CryptoCompareWebClient(URI serverUri, Draft draft) {
        super(serverUri, draft);
    }

    public CryptoCompareWebClient(URI serverURI) {
        super(serverURI);
    }

    public CryptoCompareWebClient(URI serverUri, Map<String, String> httpHeaders) {
        super(serverUri, httpHeaders);
    }

    public CryptoKafkaProducer cryptoProducer = new CryptoKafkaProducer();

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        send("{ \"action\": \"SubAdd\",\"subs\": [\"2~Coinbase~BTC~USD\"]}");
        System.out.println("opened connection");
    }

    @Override
    public void onMessage(String message) {
        System.out.println("received: " + message);

        cryptoProducer.send(message, BTC_USD_TOPIC);

        //cryptoProducer.close();
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        // The close codes are documented in class org.java_websocket.framing.CloseFrame
        System.out.println(
                "Connection closed by " + (remote ? "remote peer" : "us") + " Code: " + code + " Reason: "
                        + reason);
    }

    @Override
    public void onError(Exception ex) {
        ex.printStackTrace();
    }

    public static void main(String[] args) throws URISyntaxException {
        String apiKey = "ffc08bfd8b5bbe5e45b5809a9722ee6697ed75295ed476eaeb9f68f1c3cefccc";
        String url = "wss://streamer.cryptocompare.com/v2?api_key=" + apiKey;
        CryptoCompareWebClient c = new CryptoCompareWebClient(new URI(
                url)); // more about drafts here: http://github.com/TooTallNate/Java-WebSocket/wiki/Drafts
        c.connect();
    }
}
