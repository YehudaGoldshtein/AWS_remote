package org.example;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Map;

public class SqsService {


    public static final String LOG_TO_LOCAL = "LogToLocalQueue";

    static Map<String, String> queueUrls = new java.util.HashMap<>();

    private static final SqsClient client = SqsClient.builder()
            .region(Region.US_EAST_1)
            .build();

    public static String getSQSQueue(String queueName) {

        //first look up in hashmap
        if (queueUrls.containsKey(queueName)){
            return queueUrls.get(queueName);
        }

        //first try to get the queue assuming it exists already
        try {
            GetQueueUrlResponse response = client.getQueueUrl(
                    GetQueueUrlRequest.builder()
                            .queueName(queueName)
                            .build()
            );
            //add to hashmap
            queueUrls.put(queueName, response.queueUrl());
            return response.queueUrl();

            //if it does not exist, create it
        } catch (QueueDoesNotExistException e) {
            System.out.println("SQS Queue " + queueName + " does not exist. Creating it.");
            CreateQueueResponse response = client.createQueue(
                    CreateQueueRequest.builder()
                            .queueName(queueName)
                            .build()
            );
            queueUrls.put(queueName, response.queueUrl());
            return response.queueUrl();
        }
    }

    public static void sendMessage(String queueName, String messageBody){
        //first check if queue exists
        SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                .queueUrl(getSQSQueue(queueName))
                .messageBody(messageBody)
                .build();

        //log both url and queue name
        System.out.println("Sending message to SQS Queue: " + queueName + " URL: " + getSQSQueue(queueName));
        client.sendMessage(sendMsgRequest);
        System.out.println("Message sent to SQS: " + messageBody);
    }

    public static List<Message> getMessagesForQueue(String queueName) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(getSQSQueue(queueName))
                .maxNumberOfMessages(1)
                .waitTimeSeconds(1)
                .build();

        ReceiveMessageResponse response = client.receiveMessage(receiveMessageRequest);
        return response.messages();
    }

    public static void deleteMessage(String queueName, Message message) {
        client.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(getSQSQueue(queueName))
                .receiptHandle(message.receiptHandle())
                .build());
    }

    public static void emptyQueue(String queueName) {
        client.purgeQueue(PurgeQueueRequest.builder()
                .queueUrl(getSQSQueue(queueName))
                .build());
    }

    /**
     * Get the approximate number of messages in the queue
     * @param queueName The name of the queue
     * @return Approximate number of messages (including invisible messages)
     */
    public static int getQueueMessageCount(String queueName) {
        try {
            GetQueueAttributesResponse response = client.getQueueAttributes(
                GetQueueAttributesRequest.builder()
                    .queueUrl(getSQSQueue(queueName))
                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                   QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)
                    .build()
            );

            String visible = response.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);
            String notVisible = response.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE);

            int visibleCount = visible != null ? Integer.parseInt(visible) : 0;
            int notVisibleCount = notVisible != null ? Integer.parseInt(notVisible) : 0;

            return visibleCount + notVisibleCount;
        } catch (Exception e) {
            Logger.getLogger().log("Error getting queue message count: " + e.getMessage());
            return 0;
        }
    }
}
