package com.tkonuklar.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.util.ArrayList;
import java.util.List;

public class MessageRetriever implements RequestHandler<Message, List<Message>> {
    private AmazonDynamoDB client;
    private DynamoDBMapper mapper;

    @Override
    public List<Message> handleRequest(Message message, Context context) {
        context.getLogger().log("Initiating Message Retriever Messages GET Rest API Function");
        context.getLogger().log("Message received: {" + message.toString() + "}");
        client = AmazonDynamoDBClientBuilder.standard().build();
        mapper = new DynamoDBMapper(client);
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
        try {
            List<Message> messageList = mapper.scan(Message.class, scanExpression);
            context.getLogger().log("Messages size in DynamoDB : " + messageList.size());
            messageList.stream()
                    .forEach(msg -> context.getLogger().log("Message found in DynamoDB: {" + msg.toString() + "}"));
            return messageList;
        } catch (Exception e) {
            context.getLogger().log("Exception found, returning empty list. Error: " + e.getMessage());
            return new ArrayList<>();
        }
    }
}