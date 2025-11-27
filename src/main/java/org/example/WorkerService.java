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
import java.util.concurrent.locks.ReentrantLock;

public class WorkerService {

    // Lock to prevent race conditions when starting workers
    private static final ReentrantLock workerCreationLock = new ReentrantLock();

    public static final String WORKER_ROLE = "EMR_EC2_DefaultRole";
    static final String WORKER_TAG = "WorkerInstance";
    static final String MANAGER_TAG = "ManagerInstance";
    public static final String MANAGER_REQUEST_QUEUE = "ManagerRequestQueue";
    public static final String WORKER_REQUEST_QUEUE = "WorkerRequestQueue";
    public static final String MANAGER_TO_WORKER_REQUEST_QUEUE = "ManagerToWorkerRequestQueue";
    static final int MAX_WORKERS = 15; // Safety buffer: AWS lab limit is 19, using 15 to be safe
    static final InstanceType WORKER_INSTANCE_TYPE = InstanceType.T1_MICRO;
    static final String MAX_WORKERS_ENV = "MAX_WORKERS";
    static final String INSTANCE_LIST_EMPTY_ERROR = "Error: EC2 instance list is empty after runInstances call.";

    static final String jarName = "awsLocal-1.0.0.jar";
    static final String userDataScript =
            "#!/bin/bash\n" +
                    "cd /home/ec2-user\n" +
                    "nohup java -jar " + jarName + " > worker.log 2>&1 &\n";


    static final String userDataBase64 = Base64.getEncoder()
            .encodeToString(userDataScript.getBytes(StandardCharsets.UTF_8));

    public Ec2Client ec2 = null;

    String workerAmiId =  "ami-0deb2c104aa5011cc"; // will be dynamic from now on


    private static WorkerService instance = null;

    public static WorkerService getInstance(String accessKeyId, String secretAccessKey, String sessionToken, String workerAmiId){

        AwsSessionCredentials credentials = AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken);
        StaticCredentialsProvider provider = StaticCredentialsProvider.create(credentials);

        instance = new WorkerService();
        instance.ec2 = Ec2Client
                .builder()
                .credentialsProvider(provider)
                .region(Region.US_EAST_1)   // pick your region
                .build();

        instance.workerAmiId = workerAmiId;
        return instance;
    }

    public static WorkerService getInstance(){
        if (instance == null){
            throw new RuntimeException("WorkerService instance not initialized. Call getInstance with credentials first.");
        }
        return instance;
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
                .iamInstanceProfile(profile)
                .keyName("keyPair2")  // SSH keypair for access
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

    /**
     * Count only workers (for scaling decisions)
     * @return Number of workers in running or pending state
     */
    public int countRunningWorkers() {
        int workerCount = getRunningMachines(WORKER_TAG).size();
        Logger.getLogger().log("Current running workers: " + workerCount);
        return workerCount;
    }

    /**
     * Count total instances (workers + manager) for MAX limit check
     * @return Total number of our instances in running or pending state
     */
    public int countTotalInstances() {
        int workerCount = getRunningMachines(WORKER_TAG).size();
        int managerCount = getRunningMachines(MANAGER_TAG).size();
        int total = workerCount + managerCount;
        Logger.getLogger().log("Total instances: " + total + " (workers: " + workerCount + ", managers: " + managerCount + ")");
        return total;
    }

    /**
     * Start multiple workers (thread-safe with lock to prevent race conditions)
     * @param count Number of workers to start
     * @return List of started instances
     */
    public List<Instance> startWorkers(int count) {
        if (count <= 0) {
            return new ArrayList<>();
        }

        // Acquire lock to prevent race conditions when multiple threads try to start workers
        workerCreationLock.lock();
        try {
            // Use total instances (workers + manager) for MAX limit check
            int totalInstances = countTotalInstances();
            int availableSlots = MAX_WORKERS - totalInstances;

            if (availableSlots <= 0) {
                Logger.getLogger().log("Cannot start workers: MAX_WORKERS limit (" + MAX_WORKERS + ") reached. Total instances: " + totalInstances);
                return new ArrayList<>();
            }

            // Don't start more than available slots
            int workersToStart = Math.min(count, availableSlots);

            Logger.getLogger().log("Starting " + workersToStart + " workers (requested: " + count + ", available slots: " + availableSlots + ")");

            List<Instance> startedInstances = new ArrayList<>();

            for (int i = 0; i < workersToStart; i++) {
                try {
                    Instance worker = setupSingleWorker();
                    startedInstances.add(worker);
                    Logger.getLogger().log("Started worker " + (i + 1) + "/" + workersToStart + ": " + worker.instanceId());
                } catch (Exception e) {
                    Logger.getLogger().log("Error starting worker " + (i + 1) + ": " + e.getMessage());
                }
            }

            return startedInstances;
        } finally {
            workerCreationLock.unlock();
        }
    }

    /**
     * Terminate all running workers
     * @return Number of workers terminated
     */
    public int terminateAllWorkers() {
        List<Instance> runningWorkers = getRunningMachines(WORKER_TAG);
        int count = runningWorkers.size();

        if (count == 0) {
            Logger.getLogger().log("No workers to terminate");
            return 0;
        }

        Logger.getLogger().log("Terminating " + count + " worker(s)...");

        List<String> instanceIds = new ArrayList<>();
        for (Instance instance : runningWorkers) {
            instanceIds.add(instance.instanceId());
        }

        try {
            TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                    .instanceIds(instanceIds)
                    .build();

            ec2.terminateInstances(request);
            Logger.getLogger().log("Terminated " + count + " worker instance(s)");
            return count;
        } catch (Exception e) {
            Logger.getLogger().log("Error terminating workers: " + e.getMessage());
            return 0;
        }
    }

    /**
     * Get list of all running workers
     */
    private List<Instance> getRunningMachines(String tag) {
        List<Instance> runningWorkers = new ArrayList<>();

        List<Filter> filters = new ArrayList<>();
        filters.add(Filter.builder()
                .name("tag:Role")
                .values(tag)
                .build());
        filters.add(Filter.builder()
                .name("instance-state-name")
                .values(InstanceStateName.RUNNING.toString(), InstanceStateName.PENDING.toString())
                .build());

        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(filters)
                .build();

        try {
            DescribeInstancesResponse response = ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    runningWorkers.add(instance);
                }
            }
        } catch (Exception e) {
            Logger.getLogger().log("Error getting running workers: " + e.getMessage());
        }

        return runningWorkers;
    }

    public int terminateManager() {
        List<Instance> runningManagers = getRunningMachines(MANAGER_TAG);
        int count = runningManagers.size();

        if (count == 0) {
            Logger.getLogger().log("No manager to terminate");
            return 0;
        }

        Logger.getLogger().log("Terminating " + count + " manager(s)...");

        List<String> instanceIds = new ArrayList<>();
        for (Instance instance : runningManagers) {
            instanceIds.add(instance.instanceId());
        }

        try {
            TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                    .instanceIds(instanceIds)
                    .build();

            ec2.terminateInstances(request);
            Logger.getLogger().log("Terminated " + count + " worker instance(s)");
            return count;
        } catch (Exception e) {
            Logger.getLogger().log("Error terminating workers: " + e.getMessage());
            return 0;
        }
    }

}
