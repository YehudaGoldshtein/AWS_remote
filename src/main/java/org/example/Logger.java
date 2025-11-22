package org.example;

import java.sql.Time;

import static org.example.ManagerApp.LOCAL_TO_MANAGER_REQUEST_QUEUE;

public class Logger {
    static Logger instance;
    public static Logger getLogger(){
        if (instance == null){
            instance = new Logger();
        }
        return instance;
    }
    public void log(String message){
        System.out.println("LOG: " + getNiceTime(System.currentTimeMillis()) + "    " + message);
        try
        {SqsService.sendMessage(LOCAL_TO_MANAGER_REQUEST_QUEUE, "LOG: " + getNiceTime(System.currentTimeMillis()) + "    " + message);
        }
        catch (Exception e){}

    }

    static String getNiceTime(long millis){
        long seconds = millis / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        return String.format("%02d:%02d:%02d", hours % 24, minutes % 60, seconds % 60);
    }
}
