package com.tkonuklar.sns;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;

import java.util.List;
import java.util.UUID;

public class SnsPublisher {

    private static String TOPIC_ARN_MESSAGE = "YOUR_SNS_TOPIC_ARN_URL";
    private AmazonSNSClient snsClient;

    public static void main(String[] args) {
        SnsPublisher publisher = new SnsPublisher();
        publisher.init();
        publisher.sendMessages();
    }

    private void init() {
        snsClient = (AmazonSNSClient) AmazonSNSClientBuilder.standard().build();
    }

    private void sendMessages() {
        final var messages = List.of("Hello Medium!", "Message with Love");
        messages.stream()
                .forEach(message ->
                        snsClient.publish(TOPIC_ARN_MESSAGE, String.format("{ \"messageId\": \"%s\" , \"message\": \"%s\"}", UUID.randomUUID(), message)));

    }
}
