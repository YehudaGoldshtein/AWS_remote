package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class to track job progress and results
 * Each job represents one input file from a Local App
 */
public class JobInfo {
    
    // Static map to track all active jobs
    // Key: jobId (e.g., input file name or S3 URL)
    // Value: JobInfo instance
    private static ConcurrentHashMap<String, JobInfo> activeJobs = new ConcurrentHashMap<>();
    
    private String jobId;  // Unique identifier for this job (e.g., input file S3 URL)
    private AtomicInteger totalTasks;  // Total number of tasks in this job
    private AtomicInteger completedTasks;  // Number of completed tasks
    private List<TaskResult> results;  // List of results (one per task)
    private String inputFileS3Url;  // S3 URL of the input file
    private long startTime;  // When the job started
    
    public JobInfo(String jobId, String inputFileS3Url) {
        this.jobId = jobId;
        this.inputFileS3Url = inputFileS3Url;
        this.totalTasks = new AtomicInteger(0);
        this.completedTasks = new AtomicInteger(0);
        this.results = new ArrayList<>();
        this.startTime = System.currentTimeMillis();
    }
    
    /**
     * Create or get a job
     */
    public static JobInfo getOrCreateJob(String jobId, String inputFileS3Url) {
        return activeJobs.computeIfAbsent(jobId, k -> new JobInfo(jobId, inputFileS3Url));
    }
    
    /**
     * Get a job by ID
     */
    public static JobInfo getJob(String jobId) {
        return activeJobs.get(jobId);
    }
    
    /**
     * Remove a completed job
     */
    public static void removeJob(String jobId) {
        activeJobs.remove(jobId);
    }
    
    /**
     * Get all active jobs
     */
    public static ConcurrentHashMap<String, JobInfo> getAllJobs() {
        return activeJobs;
    }
    
    /**
     * Set the total number of tasks for this job
     */
    public void setTotalTasks(int total) {
        this.totalTasks.set(total);
    }
    
    /**
     * Increment total tasks (when a new task is created)
     */
    public void incrementTotalTasks() {
        this.totalTasks.incrementAndGet();
    }
    
    /**
     * Add a result and increment completed tasks
     */
    public synchronized void addResult(TaskResult result) {
        this.results.add(result);
        this.completedTasks.incrementAndGet();
    }
    
    /**
     * Check if job is complete
     */
    public boolean isComplete() {
        return completedTasks.get() >= totalTasks.get() && totalTasks.get() > 0;
    }
    
    /**
     * Get completion percentage
     */
    public double getCompletionPercentage() {
        if (totalTasks.get() == 0) return 0.0;
        return (completedTasks.get() * 100.0) / totalTasks.get();
    }
    
    // Getters
    public String getJobId() { return jobId; }
    public int getTotalTasks() { return totalTasks.get(); }
    public int getCompletedTasks() { return completedTasks.get(); }
    public List<TaskResult> getResults() { return results; }
    public String getInputFileS3Url() { return inputFileS3Url; }
    public long getStartTime() { return startTime; }
    
    /**
     * Inner class to represent a single task result
     */
    public static class TaskResult {
        private String analysisType;
        private String inputUrl;
        private String outputS3Url;  // null if error
        private String errorMessage;  // null if success
        private boolean isError;
        
        public TaskResult(String analysisType, String inputUrl, String outputS3Url) {
            this.analysisType = analysisType;
            this.inputUrl = inputUrl;
            this.outputS3Url = outputS3Url;
            this.errorMessage = null;
            this.isError = false;
        }
        
        public TaskResult(String analysisType, String inputUrl, String errorMessage, boolean isError) {
            this.analysisType = analysisType;
            this.inputUrl = inputUrl;
            this.outputS3Url = null;
            this.errorMessage = errorMessage;
            this.isError = true;
        }
        
        // Getters
        public String getAnalysisType() { return analysisType; }
        public String getInputUrl() { return inputUrl; }
        public String getOutputS3Url() { return outputS3Url; }
        public String getErrorMessage() { return errorMessage; }
        public boolean isError() { return isError; }
    }
}

