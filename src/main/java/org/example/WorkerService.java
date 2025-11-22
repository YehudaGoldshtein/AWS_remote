package org.example;

import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class WorkerService {


    public static final String WORKER_ROLE = "EMR_EC2_DefaultRole";
    static final String WORKER_TAG = "WorkerInstance";
    static final String workerAmiId =  "ami-071e30579bb36ab78"; // with java 17, more logs
    public static final String MANAGER_REQUEST_QUEUE = "ManagerRequestQueue";
    public static final String WORKER_REQUEST_QUEUE = "WorkerRequestQueue";
    static final int MAX_WORKERS = 19;//amazon labm limit is 19 instances per account
    static final InstanceType WORKER_INSTANCE_TYPE = InstanceType.T1_MICRO;
    static final String MAX_WORKERS_ENV = "MAX_WORKERS";
    static final String INSTANCE_LIST_EMPTY_ERROR = "Error: EC2 instance list is empty after runInstances call.";

    static final String jarName = "AWSRemote-1.0-SNAPSHOT-shaded.jar";
    static final String userDataScript =
            "#!/bin/bash\n" +
                    "cd /home/ec2-user\n" +
                    "nohup java -jar " + jarName + " > worker.log 2>&1 &\n";


    static final String userDataBase64 = Base64.getEncoder()
            .encodeToString(userDataScript.getBytes(StandardCharsets.UTF_8));

    public Ec2Client ec2 = null;

    private static WorkerService instance = null;

    public static WorkerService getInstance(String accessKeyId, String secretAccessKey, String sessionToken){

        AwsSessionCredentials credentials = AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken);
        StaticCredentialsProvider provider = StaticCredentialsProvider.create(credentials);

        instance = new WorkerService();
        instance.ec2 = Ec2Client
                .builder()
                .credentialsProvider(provider)
                .region(Region.US_EAST_1)   // pick your region
                .build();

        return instance;
    }

    public static WorkerService getInstance(){
        if (instance == null){
            throw new RuntimeException("WorkerService instance not initialized. Call getInstance with credentials first.");
        }
        return instance;
    }

    @NotNull
    public Instance getSingleWorker(){

        List<Filter> filters = new ArrayList<>();

        filters.add(Filter.builder()
                .name("tag:Role")
                .values(WORKER_TAG)
                .build());

        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(filters)
                .build();

        DescribeInstancesResponse response = Ec2Client.create().describeInstances(request);

        int activeWorkers = 0;
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                Logger.getLogger().log("Found running worker instance: " + instance.instanceId());
                //get instance state
                InstanceStateName state =  instance.state().name();
                Logger.getLogger().log("Instance state: " + state.toString());
                if (state == InstanceStateName.STOPPED){
                    Logger.getLogger().log("found stopped worker instance." + instance.instanceId());
                    //return this instance
                    return instance;
                }
                activeWorkers++;
            }
        }

        Logger.getLogger().log("found " + activeWorkers + " active workers");
        if (activeWorkers < MAX_WORKERS){
            return setupSingleWorker();
        }
        else throw new RuntimeException(MAX_WORKERS_ENV + " limit reached. No new workers will be started.");

    }

//    public static Instance startSingleWorker(){
//        Instance worker =  setupSingleWorker();
//        worker.da
//    }
//    @NotNull


    public Instance setupSingleWorker(){
        IamInstanceProfileSpecification profile =
                IamInstanceProfileSpecification.builder()
                        .name(WORKER_ROLE)  // or whatever role name you picked
                        .build();


        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(WORKER_INSTANCE_TYPE)
                .imageId(workerAmiId)
                 .iamInstanceProfile(profile) //<-- REMOVE THIS LINE
                .maxCount(1)
                .minCount(1)
                .userData(userDataBase64)
                .metadataOptions(InstanceMetadataOptionsRequest.builder()
                        .instanceMetadataTags(InstanceMetadataTagsState.ENABLED)
                        .build())
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(Tag.builder()
                                .key("Role")
                                .value(WORKER_TAG)
                                .build())
                        .build())
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        List<Instance> instances = response.instances();
        for (Instance instance : instances) {
            Logger.getLogger().log(
                    "Successfully started EC2 instance " + instance.instanceId() + " based on AMI " + instance.imageId());
            return instance;
        }
        Logger.getLogger().log("Error: EC2 instance list is empty after runInstances call.");
        Logger.getLogger().log("RunInstancesResponse: " + response);

        throw new RuntimeException(INSTANCE_LIST_EMPTY_ERROR);
    }



}
