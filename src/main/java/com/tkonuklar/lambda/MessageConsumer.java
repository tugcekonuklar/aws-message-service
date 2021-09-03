package com.tkonuklar.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MessageConsumer implements RequestHandler<SQSEvent, String> {

    private ObjectMapper objMapper;

    @Override
    public String handleRequest(SQSEvent event, Context context) {
        context.getLogger().log("Initiating Message Consumer Function");
        objMapper = new ObjectMapper();
        try {
            AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard().build();
            DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB);
            for (SQSEvent.SQSMessage msg : event.getRecords()) {
                context.getLogger().log("Message received: " + msg.getBody());
                Message message = objMapper.readValue(msg.getBody(), Message.class);
                mapper.save(message);
                context.getLogger().log("Message was saved successfully: " + message);
            }
        } catch (Exception ex) {
            context.getLogger().log("FAILED! : " + ex.getMessage());
        }
        return event.toString();
    }
}
