package com.slimgears.rxrepo.test;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Optional;

public class DockerUtils {
    private static String defaultComposeFile = "docker-compose.yaml";

    public static void start() {
        start(defaultComposeFile);
    }

    public static void stop() {
        stop(defaultComposeFile);
    }

    public static void start(String composeFilePath) {
        System.out.println("Starting container");
        try {
            execute("docker-compose", "-f", composeFilePath, "up", "-d");
            Thread.sleep(8000);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static void stop(String composeFilePath) {
        System.out.println("Stopping container");
        if (System.getProperty("tearDown.keepContainer") != null) {
            return;
        }

        execute("docker-compose", "-f", composeFilePath, "down");
    }

    public static boolean isAvailable() {
        return Optional.ofNullable(execute("docker-compose", "--version"))
                .filter(out -> out.startsWith("docker-compose version"))
                .isPresent();
    }

    private static @Nullable String execute(String... cmdLine) {
        StringBuilder stdOut = new StringBuilder();
        try {
            Process proc = Runtime.getRuntime().exec(cmdLine);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stdOut.append(line).append("\n");
                    System.out.println(line);
                }
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getErrorStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.err.println(line);
                }
            }
            proc.waitFor();
        } catch (IOException e) {
            return null;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        return stdOut.toString();
    }
}
