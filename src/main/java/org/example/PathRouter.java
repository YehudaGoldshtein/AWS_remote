package org.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class PathRouter {

    static String managerTag = "ManagerInstance";


    public static void CheckRoutAndStart(String[] args){

    }

    public boolean amIManager(){
        //check my own ec2 tags
        return false;

    }


    private static final String METADATA_BASE = "http://169.254.169.254/latest/meta-data";

    public static String getInstanceTag(String tagKey) throws Exception {
        String url = METADATA_BASE + "/tags/instance/" + tagKey;
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(1000);
        conn.setReadTimeout(1000);

        try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            return in.readLine();  // e.g. "ManagerInstance"
        }
    }

}
