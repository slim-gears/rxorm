package com.slimgears.rxrepo.test;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class DockerUtils {
    private static String defaultComposeFile = "docker-compose.yaml";

    public static AutoCloseable withContainer() {
        return withContainer(defaultComposeFile);
    }

    public static AutoCloseable withContainer(String composeFile) {
        start(composeFile);
        return () -> stop(composeFile);
    }

    public static void start() {
        start(defaultComposeFile);
    }

    public static void stop() {
        stop(defaultComposeFile);
    }

    public static void start(String composeFilePath) {
        try {
            Process proc = Runtime.getRuntime().exec(new String[]{"docker-compose", "-f", composeFilePath, "up", "-d"});
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getErrorStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.err.println(line);
                }
            }
            Thread.sleep(5000);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static void stop(String composeFilePath) {
        if (System.getProperty("tearDown.keepContainer") != null) {
            return;
        }

        try {
            Runtime.getRuntime().exec(new String[]{"docker-compose", "-f", composeFilePath, "down"}).waitFor();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
