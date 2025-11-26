package org.example;

import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.example.WorkerService.MANAGER_TO_WORKER_REQUEST_QUEUE;

public class ManagerApp {

    public static final String LOCAL_TO_MANAGER_REQUEST_QUEUE = "LocalToManagerRequestQueue";
    public static final String MANAGER_TO_LOCAL_REQUEST_QUEUE = "ManagerToLocalRequestQueue";
    public static final String WORKER_TO_MANAGER_REQUEST_QUEUE = "WorkerToManagerRequestQueue";

    // Default files per worker (if not specified in message)
    private static final int DEFAULT_FILES_PER_WORKER = 5;

    // Store WorkerService instance for scaling operations
    private static WorkerService workerService = null;

    // ExecutorService for parallel request handling
    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    // Termination flag
    private static volatile boolean shouldTerminate = false;

    public static void run(String[] args){
        if (args.length != 4){
            Logger.getLogger().log("Invalid arguments. Usage: <accessKeyId> <secretAccessKey> <sessionToken> <workerAMI>");
            Logger.getLogger().log("got args: " + String.join(", ", args));
            return;
        }
        try {
            workerService = WorkerService.getInstance(args[0], args[1], args[2], args[3]);
            // Initialize worker service (test connection)
            Logger.getLogger().log("WorkerService initialized successfully");
        }
        catch (RuntimeException e){
            Logger.getLogger().log("could not setup worker instance. error is: " + e.getMessage()+ " Exiting...");
            System.out.println("Manager setup failed.with error: " + e.getMessage() + " Exiting...");
            return;
        }

        // Thread for handling Local App messages (parallel processing)
        new Thread(()->{
            while (ExpectingMoreMessagesFromLocalApps()){
                List<Message> messages = SqsService.getMessagesForQueue(LOCAL_TO_MANAGER_REQUEST_QUEUE);
                if (!messages.isEmpty()){
                    for (Message message : messages) {
                        // Check for termination message
                        if (message.body().equals("TERMINATE")) {
                            Logger.getLogger().log("Received termination request from Local App");
                            SqsService.deleteMessage(LOCAL_TO_MANAGER_REQUEST_QUEUE, message);
                            shouldTerminate = true;
                            // Continue processing to handle termination flow
                            continue;
                        }

                        // If termination is requested, don't accept new jobs

                        // Delete message immediately to avoid reprocessing
                        SqsService.deleteMessage(LOCAL_TO_MANAGER_REQUEST_QUEUE, message);

                        // Process in parallel using ExecutorService
                        executorService.submit(() -> {
                            try {
                                Logger.getLogger().log("Processing local app message in parallel: " + message.body());
                                handleLocalAppMessage(message);
                            } catch (Exception e) {
                                Logger.getLogger().log("Error processing local app message: " + e.getMessage());
                                SqsService.sendMessage(MANAGER_TO_LOCAL_REQUEST_QUEUE,
                                    "ERROR;" + message.body() + ";" + e.getMessage());
                            }
                        });
                    }
                }

                // If termination requested, check if all jobs are complete
                if (shouldTerminate) {
                    if (JobInfo.getAllJobs().isEmpty()) {
                        Logger.getLogger().log("All jobs completed, proceeding with termination");
                        break;
                    } else {
                        int activeJobs = JobInfo.getAllJobs().size();
                        Logger.getLogger().log("Waiting for " + activeJobs + " active job(s) to complete...");
                    }
                }

                // Small sleep to avoid busy waiting
                try {
                    Thread.sleep(100); // 100ms
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            postProccess();
        })
                .start();

        while (ExpectingMoreMessagesFromWorkers()){
            //get messages from workers, name of queue is misleading, will fix later
            List<Message> messages = SqsService.getMessagesForQueue(WORKER_TO_MANAGER_REQUEST_QUEUE);
            if (!messages.isEmpty()){
                for (Message message : messages) {
                    Logger.getLogger().log("Received message: " + message.body());
                    handleWorkerMessage(message);
                    SqsService.deleteMessage(WORKER_TO_MANAGER_REQUEST_QUEUE, message);
                }
            }
            else SqsService.sendMessage(MANAGER_TO_LOCAL_REQUEST_QUEUE, "I Am the manager and i have no messages from workers at this time.");

        }
        postProccess();
        if (shouldTerminate){
            terminateSelf();
        }

    }

    //temp var, remove later
    static int expectedNumberOfMessages = 10;
    private static boolean ExpectingMoreMessagesFromWorkers() {
        //for now, always return true,
        //suggested implementation: have a list of returned files from workers, when reached expected number, return false.
        //manage that list using the handleWorkerMessage method.
        if (expectedNumberOfMessages > 0){
            expectedNumberOfMessages--;
            return true;
        }
        return false;
    }

    static void handleWorkerMessage(Message message){
        String messageBody = message.body();
        Logger.getLogger().log("Handling worker message: " + messageBody);

        // Parse worker message
        // Success format: "ANALYSIS_TYPE;INPUT_URL;OUTPUT_S3_URL"
        // Error format: "ERROR;ANALYSIS_TYPE;INPUT_URL;ERROR_MESSAGE"
        String[] parts = messageBody.split(";");

        if (parts.length < 3) {
            Logger.getLogger().log("Warning: Invalid worker message format: " + messageBody);
            return;
        }

        boolean isError = parts[0].equals("ERROR");
        String analysisType = isError ? parts[1] : parts[0];
        String inputUrl = isError ? parts[2] : parts[1];
        String outputOrError = isError ? (parts.length > 3 ? parts[3] : "Unknown error") : parts[2];

        Logger.getLogger().log("Parsed: analysisType=" + analysisType + ", inputUrl=" + inputUrl +
                              ", isError=" + isError + ", output/error=" + outputOrError);

        // Find which job this task belongs to
        // We search all jobs to find one that might have this URL
        // Note: This assumes each URL is unique per job, or we match by URL pattern
        JobInfo job = findJobForTask(analysisType, inputUrl);

        if (job == null) {
            Logger.getLogger().log("Warning: Could not find job for task: " + analysisType + " -> " + inputUrl);
            return;
        }

        // Create result and add to job
        JobInfo.TaskResult result;
        if (isError) {
            result = new JobInfo.TaskResult(analysisType, inputUrl, outputOrError, true);
        } else {
            result = new JobInfo.TaskResult(analysisType, inputUrl, outputOrError);
        }

        job.addResult(result);
        Logger.getLogger().log("Added result to job " + job.getJobId() +
                             ". Progress: " + job.getCompletedTasks() + "/" + job.getTotalTasks() +
                             " (" + String.format("%.1f", job.getCompletionPercentage()) + "%)");

        // Check if job is complete
        if (job.isComplete()) {
            Logger.getLogger().log("Job " + job.getJobId() + " is complete! Generating HTML...");
            handleJobCompletion(job);
        }
    }

    /**
     * Find the job that contains this task
     * Searches all active jobs to find one with matching URL
     */
    private static JobInfo findJobForTask(String analysisType, String inputUrl) {
        // Search all active jobs
        for (JobInfo job : JobInfo.getAllJobs().values()) {
            // Check if any result or pending task matches
            // For now, we'll match by checking if URL appears in any result
            // This is a simple approach - could be improved
            for (JobInfo.TaskResult result : job.getResults()) {
                if (result.getInputUrl().equals(inputUrl) && result.getAnalysisType().equals(analysisType)) {
                    // Already processed, return the job
                    return job;
                }
            }

            // If job has this URL pattern, it might belong to this job
            // We'll use a simple heuristic: if the job is recent and we haven't found a match, assign it
            // For now, let's just check all jobs and return the first one that seems to match
            // This is a simplification - in production, you'd want to track task-to-job mapping
        }

        // If no exact match, return the most recent job (heuristic)
        // This assumes tasks arrive in order
        JobInfo mostRecent = null;
        long mostRecentTime = 0;
        for (JobInfo job : JobInfo.getAllJobs().values()) {
            if (job.getStartTime() > mostRecentTime && !job.isComplete()) {
                mostRecent = job;
                mostRecentTime = job.getStartTime();
            }
        }

        return mostRecent;
    }

    /**
     * Handle job completion: generate HTML and notify Local App
     */
    private static void handleJobCompletion(JobInfo job) {
        try {
            // Generate HTML file
            String htmlContent = generateHTML(job);

            // Upload HTML to S3
            String htmlS3Url = S3Service.uploadHTMLFile(htmlContent, job.getJobId());

            if (htmlS3Url != null) {
                Logger.getLogger().log("HTML file uploaded to S3: " + htmlS3Url);

                // Send completion message to Local App
                // Format: "DONE;INPUT_FILE_S3_URL;HTML_S3_URL"
                String completionMessage = "DONE;" + job.getInputFileS3Url() + ";" + htmlS3Url;
                SqsService.sendMessage(MANAGER_TO_LOCAL_REQUEST_QUEUE, completionMessage);
                Logger.getLogger().log("Sent completion message to Local App: " + completionMessage);

                // Remove completed job
                JobInfo.removeJob(job.getJobId());
            } else {
                Logger.getLogger().log("Error: Failed to upload HTML file to S3");
                SqsService.sendMessage(MANAGER_TO_LOCAL_REQUEST_QUEUE,
                    "ERROR;" + job.getInputFileS3Url() + ";Failed to generate HTML file");
            }
        } catch (Exception e) {
            Logger.getLogger().log("Error handling job completion: " + e.getMessage());
            SqsService.sendMessage(MANAGER_TO_LOCAL_REQUEST_QUEUE,
                "ERROR;" + job.getInputFileS3Url() + ";" + e.getMessage());
        }
    }

    /**
     * Generate HTML file from job results
     */
    private static String generateHTML(JobInfo job) {
        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html>\n");
        html.append("<html>\n<head>\n");
        html.append("<title>Text Analysis Results</title>\n");
        html.append("<style>body { font-family: Arial, sans-serif; margin: 20px; }</style>\n");
        html.append("</head>\n<body>\n");
        html.append("<h1>Text Analysis Results</h1>\n");
        html.append("<p>Input File: <a href=\"").append(job.getInputFileS3Url()).append("\">").append(job.getInputFileS3Url()).append("</a></p>\n");
        html.append("<p>Total Tasks: ").append(job.getTotalTasks()).append(" | Completed: ").append(job.getCompletedTasks()).append("</p>\n");
        html.append("<hr>\n");
        html.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");
        html.append("<tr><th>Analysis Type</th><th>Input File</th><th>Output File / Error</th></tr>\n");

        for (JobInfo.TaskResult result : job.getResults()) {
            html.append("<tr>\n");
            html.append("<td>").append(result.getAnalysisType()).append("</td>\n");
            html.append("<td><a href=\"").append(result.getInputUrl()).append("\">").append(result.getInputUrl()).append("</a></td>\n");

            if (result.isError()) {
                html.append("<td style=\"color: red;\">").append(result.getErrorMessage()).append("</td>\n");
            } else {
                html.append("<td><a href=\"").append(result.getOutputS3Url()).append("\">").append(result.getOutputS3Url()).append("</a></td>\n");
            }

            html.append("</tr>\n");
        }

        html.append("</table>\n");
        html.append("</body>\n</html>");

        return html.toString();
    }
    static void postProccess(){
        Logger.getLogger().log("Manager post processing started.");
        //suggested implementation: terminate all workers, clean up SQS queues, send final message to local client.

        // Shutdown ExecutorService gracefully
        if (executorService != null) {
            Logger.getLogger().log("Shutting down ExecutorService...");
            executorService.shutdown();
            try {
                // Wait for running tasks to complete (with timeout)
                if (!executorService.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)) {
                    Logger.getLogger().log("ExecutorService did not terminate gracefully, forcing shutdown...");
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                Logger.getLogger().log("ExecutorService shutdown interrupted: " + e.getMessage());
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Terminate all workers
        if (workerService != null) {
            Logger.getLogger().log("Terminating all workers...");
            int terminatedCount = workerService.terminateAllWorkers();
            Logger.getLogger().log("Terminated " + terminatedCount + " worker(s)");
        }

        SqsService.sendMessage(MANAGER_TO_LOCAL_REQUEST_QUEUE, "I Am the manager and i have finished all my work. Exiting...");
        Logger.getLogger().log("Manager post processing finished.");
    }

    private static boolean ExpectingMoreMessagesFromLocalApps() {
        // Return false if termination is requested and all jobs are complete
            return !JobInfo.getAllJobs().isEmpty();

        // Otherwise, always return true to keep listening
        return true;
    }

    static void handleLocalAppMessage(Message message){
        Logger.getLogger().log("Handling local app message: " + message.body());

        // Message from Local App: format is "S3_URL" or "S3_URL;n" (n = files per worker)
        String messageBody = message.body();
        String s3Url;
        int filesPerWorker = DEFAULT_FILES_PER_WORKER;

        // Parse message: check if it includes n parameter
        if (messageBody.contains(";")) {
            String[] parts = messageBody.split(";");
            s3Url = parts[0];
            if (parts.length > 1) {
                try {
                    filesPerWorker = Integer.parseInt(parts[1].trim());
                    Logger.getLogger().log("Files per worker (n): " + filesPerWorker);
                } catch (NumberFormatException e) {
                    Logger.getLogger().log("Warning: Invalid n parameter, using default: " + DEFAULT_FILES_PER_WORKER);
                }
            }
        } else {
            s3Url = messageBody;
            Logger.getLogger().log("Using default files per worker (n): " + DEFAULT_FILES_PER_WORKER);
        }

        // Create a job to track this input file
        String jobId = s3Url;  // Use S3 URL as job ID
        JobInfo job = JobInfo.getOrCreateJob(jobId, s3Url);
        Logger.getLogger().log("Created/retrieved job: " + jobId);

        File file = S3Service.downloadFile(s3Url);

        if (file == null) {
            Logger.getLogger().log("Error: Failed to download input file from S3: " + s3Url);
            SqsService.sendMessage(MANAGER_TO_LOCAL_REQUEST_QUEUE, "ERROR;Failed to download input file from S3");
            JobInfo.removeJob(jobId);
            return;
        }

        // Parse input file: format is ANALYSIS_TYPE\tURL (tab-separated)
        int taskCount = 0;
        try {
            BufferedReader reader = new BufferedReader(new java.io.FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                // Skip empty lines
                if (line.trim().isEmpty()) {
                    continue;
                }

                // Validate format: should be ANALYSIS_TYPE\tURL
                String[] parts = line.split("\t");
                if (parts.length != 2) {
                    Logger.getLogger().log("Warning: Invalid line format (expected ANALYSIS_TYPE\\tURL): " + line);
                    continue;
                }

                String analysisType = parts[0].trim();
                String url = parts[1].trim();

                // Validate analysis type
                if (!analysisType.equals("POS") && !analysisType.equals("CONSTITUENCY") && !analysisType.equals("DEPENDENCY")) {
                    Logger.getLogger().log("Warning: Invalid analysis type: " + analysisType + " in line: " + line);
                    continue;
                }

                // Validate URL
                if (url.isEmpty() || (!url.startsWith("http://") && !url.startsWith("https://"))) {
                    Logger.getLogger().log("Warning: Invalid URL: " + url + " in line: " + line);
                    continue;
                }

                Logger.getLogger().log("Creating task: " + analysisType + " -> " + url);

                // Send task to worker queue (format: ANALYSIS_TYPE\tURL)
                SqsService.sendMessage(MANAGER_TO_WORKER_REQUEST_QUEUE, line);
                taskCount++;
                job.incrementTotalTasks();  // Track task creation
            }
            reader.close();

            // Set total tasks for the job
            job.setTotalTasks(taskCount);

            Logger.getLogger().log("Created " + taskCount + " tasks from input file for job: " + jobId);

            // Clean up downloaded file
            if (file.exists()) {
                file.delete();
            }

            // Worker scaling: check queue and start workers if needed
            scaleWorkers(filesPerWorker);

        } catch (IOException e) {
            Logger.getLogger().log("Error reading input file: " + e.getMessage());
            SqsService.sendMessage(MANAGER_TO_LOCAL_REQUEST_QUEUE, "ERROR;Failed to read input file: " + e.getMessage());
            JobInfo.removeJob(jobId);
        }
    }

    /**
     * Scale workers based on queue size and n (files per worker)
     * According to assignment: create a worker for every n messages
     */
    private static void scaleWorkers(int filesPerWorker) {
        if (workerService == null) {
            Logger.getLogger().log("Warning: WorkerService not initialized, cannot scale workers");
            return;
        }

        try {
            // Count messages in worker queue
            int queueMessageCount = SqsService.getQueueMessageCount(MANAGER_TO_WORKER_REQUEST_QUEUE);
            Logger.getLogger().log("Worker queue has " + queueMessageCount + " messages");

            // Calculate needed workers: ceil(queueMessageCount / filesPerWorker)
            int neededWorkers = (int) Math.ceil((double) queueMessageCount / filesPerWorker);
            Logger.getLogger().log("Needed workers: " + neededWorkers + " (based on " + queueMessageCount + " messages / " + filesPerWorker + " per worker)");

            // Count currently running workers
            int currentWorkers = workerService.countRunningWorkers();
            Logger.getLogger().log("Current running workers: " + currentWorkers);

            // Calculate how many new workers to start
            int workersToStart = neededWorkers - currentWorkers;

            if (workersToStart > 0) {
                Logger.getLogger().log("Starting " + workersToStart + " new workers...");
                List<software.amazon.awssdk.services.ec2.model.Instance> started = workerService.startWorkers(workersToStart);
                Logger.getLogger().log("Successfully started " + started.size() + " workers");
            } else {
                Logger.getLogger().log("No new workers needed. Current: " + currentWorkers + ", Needed: " + neededWorkers);
            }

        } catch (Exception e) {
            Logger.getLogger().log("Error scaling workers: " + e.getMessage());
        }
    }

    static void terminateSelf(){
        Logger.getLogger().log("Manager terminating itself as per request.");
        // Clean up resources if needed
        WorkerService.getInstance().terminateManager();
        //shut down via ec2

    }
}
