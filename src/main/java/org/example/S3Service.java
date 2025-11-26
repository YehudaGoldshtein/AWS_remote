package org.example;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;

public class S3Service {

    //static instance
    static S3Service instance;

    static String bucketName = "yehuda-awsremote-20251113";


    static S3Client s3 = S3Client.builder()
            .region(Region.US_EAST_1)
            .build();

    public static void handleCompletion(String fileName, Message message){
        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();

        String key = message.body().split(";")[1] + "_output.txt";
        File outputFile = new File("output_" + fileName);
        try {
            s3.getObject(GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build(),
                    outputFile.toPath());
            Logger.getLogger().log("Output file downloaded: " + outputFile.getAbsolutePath());
        } catch (Exception e) {
            Logger.getLogger().log("Error downloading output file: " + e.getMessage());
        }
    }

    public static String uploadFile(File file){


        String key = file.getName();
        try {
            //i manually created this bucket in AWS console
            s3.putObject(builder -> builder.bucket(bucketName).key(key).build(),
                    file.toPath());
            Logger.getLogger().log("File uploaded to S3: " + key);
            return "s3://" + bucketName + "/" + key;
        } catch (Exception e) {
            Logger.getLogger().log("Error uploading file to S3: " + e.getMessage());
            return null;
        }
    }

    public static File downloadFile(String s3Path) {
        String key = s3Path.substring(s3Path.lastIndexOf("/") + 1);
        //make a fileName based on time stamp to avoid collisions
        String downloadAsFileName = "downloaded_" + System.currentTimeMillis();
        File outputFile = new File(downloadAsFileName);

        // --- FIX START: Create the directory if it doesn't exist ---
        if (outputFile.getParentFile() != null && !outputFile.getParentFile().exists()) {
            outputFile.getParentFile().mkdirs();
        }
        // --- FIX END ---

        try {
            s3.getObject(GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build(),
                    outputFile.toPath());
            Logger.getLogger().log("File downloaded from S3: " + outputFile.getAbsolutePath());
        } catch (Exception e) {
            Logger.getLogger().log("Error downloading file from S3: " + e.getMessage());
            // Return null or throw exception so the main loop knows it failed
            return null;
        }
        return outputFile;
    }

    /**
     * Upload HTML content to S3
     * @param htmlContent The HTML content as a string
     * @param jobId The job ID to use in the filename
     * @return S3 URL of the uploaded file, or null on error
     */
    public static String uploadHTMLFile(String htmlContent, String jobId) {
        // Generate filename from jobId (extract filename from S3 URL)
        String filename = "results_" + System.currentTimeMillis() + ".html";
        if (jobId != null && jobId.contains("/")) {
            // Extract meaningful part from S3 URL
            String baseName = jobId.substring(jobId.lastIndexOf("/") + 1);
            if (baseName.contains(".")) {
                baseName = baseName.substring(0, baseName.lastIndexOf("."));
            }
            filename = baseName + "_results_" + System.currentTimeMillis() + ".html";
        }

        File file = null;
        try {
            // Create temp file
            file = File.createTempFile("html_", ".html");

            // Write HTML content to file
            Files.write(file.toPath(), htmlContent.getBytes(StandardCharsets.UTF_8));
            Logger.getLogger().log("Created HTML temp file: " + file.getAbsolutePath());

            // Rename to have meaningful name
            String tempDir = System.getProperty("java.io.tmpdir");
            File renamedFile = new File(tempDir, filename);
            file.renameTo(renamedFile);
            file = renamedFile;

            // Upload using existing method
            String s3Url = uploadFile(file);

            // Clean up
            if (file.exists()) {
                file.delete();
            }

            return s3Url;

        } catch (Exception e) {
            Logger.getLogger().log("Error uploading HTML file: " + e.getMessage());

            // Clean up on error
            if (file != null && file.exists()) {
                try {
                    file.delete();
                } catch (Exception deleteEx) {
                    // Ignore
                }
            }
            return null;
        }
    }
}
