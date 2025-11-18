package org.example;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class Main {

    public static final String MANAGER_REQUEST_QUEUE = "ManagerRequestQueue";

    public static void main(String[] args) {
        System.out.println("Hello world!");
        String role = null;
        try {
            role = PathRouter.getInstanceTag("Role");
        } catch (Exception e) {
            System.out.println("Error retrieving instance tag: " + e.getMessage());
        }
        SqsService.sendMessage(MANAGER_REQUEST_QUEUE, "Hello from remote Java app! Role: " + role);

    }

    public static void start() {
        // Build SQS client (uses your credentials/default profile)
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();

        String queueUrl;

        try {
            // Try to get existing queue
            GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                    .queueName(MANAGER_REQUEST_QUEUE)
                    .build();

            GetQueueUrlResponse getQueueUrlResponse = sqsClient.getQueueUrl(getQueueUrlRequest);
            queueUrl = getQueueUrlResponse.queueUrl();
            System.out.println("Queue exists. URL: " + queueUrl);

        } catch (QueueDoesNotExistException e) {
            // If queue doesn't exist, create it
            System.out.println("Queue does not exist. Creating queue: " + MANAGER_REQUEST_QUEUE);

            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(MANAGER_REQUEST_QUEUE)
                    .build();

            queueUrl = sqsClient.createQueue(createQueueRequest).queueUrl();
            System.out.println("Queue created. URL: " + queueUrl);
        }

        // Send a message to the queue
        String messageBody = "Hello from remote Java app!";
        SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build();

        sqsClient.sendMessage(sendMsgRequest);
        System.out.println("Message sent to SQS: " + messageBody);

        // Cleanly close client and exit
        sqsClient.close();
        System.out.println("Shutting down.");
    }
}
