package org.example;

import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;


import static org.example.WorkerService.MANAGER_REQUEST_QUEUE;

public class ManagerApp {

    public static final String LOCAL_TO_MANAGER_REQUEST_QUEUE = "LocalToManagerRequestQueue";

    public static void run(String[] args){
        if (args.length != 4){
            Logger.getLogger().log("Invalid arguments. Usage: <accessKeyId> <secretAccessKey> <sessionToken>");
            Logger.getLogger().log("got args: " + String.join(", ", args));

            return;
        }
        try {
            WorkerService workerService = WorkerService.getInstance(args[1], args[2], args[3]);
            Instance workerInstance = workerService.getSingleWorker();
        }
        catch (RuntimeException e){
            SqsService.sendMessage(LOCAL_TO_MANAGER_REQUEST_QUEUE, "I Am the manager and i could not setup worker instance. error is: " + e.getMessage()+ " Exiting...");
            System.out.println("Manager setup failed.with error: " + e.getMessage() + " Exiting...");
            return;
        }

        while (true){

            List<Message> messages = SqsService.getMessagesForQueue(MANAGER_REQUEST_QUEUE);
            if (!messages.isEmpty()){
                for (Message message : messages) {
                    Logger.getLogger().log("Received message: " + message.body());
                    SqsService.sendMessage(LOCAL_TO_MANAGER_REQUEST_QUEUE, "I Am the manager and here is a message forwarded from a worker: " + message.body());
                }
            }
            else SqsService.sendMessage(LOCAL_TO_MANAGER_REQUEST_QUEUE, "I Am the manager and i have no messages from workers at this time.");

        }

    }
}
