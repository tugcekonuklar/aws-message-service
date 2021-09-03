package com.tkonuklar.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MessageConsumer implements RequestHandler<SQSEvent, String> {
    @Override
    public String handleRequest(SQSEvent event, Context context) {
        context.getLogger().log("Initiating Message Consumer Function");
        ObjectMapper objMapper = new ObjectMapper();
        try {
            for (SQSEvent.SQSMessage msg : event.getRecords()) {
                System.out.println(msg.getBody());
                context.getLogger().log("Message received: " + msg.getBody());
                AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard().build();
                DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB);
                Message message = objMapper.readValue(msg.getBody(), Message.class);
                message.setMessageId(null);
                mapper.save(message);
                context.getLogger().log("Message saved successfully: " + message);
            }
        } catch (Exception ex) {
            context.getLogger().log("FAILED! : " + ex.getMessage());
        }
        return event.toString();
//        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard().build();
//        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB);
//        for (SQSEvent.SQSMessage msg : event.getRecords()) {
//            Message message = new Message();
//            message.setMessage(msg.getBody());
//            context.getLogger().log("Message received: {" + message.toString() + "}");
//            mapper.save(message);
//        }
//        context.getLogger().log("Returning messageID: \"" + message.getMessageId() + "\"");
//        return message.getMessageId();
    }
}
