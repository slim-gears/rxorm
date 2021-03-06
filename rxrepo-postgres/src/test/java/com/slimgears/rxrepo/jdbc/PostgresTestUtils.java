package com.slimgears.rxrepo.jdbc;

import com.slimgears.rxrepo.test.DockerUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgresTestUtils {
    public static final String connectionUrl = "jdbc:postgresql://localhost/test_db?user=root&password=root";
    public static final String schemaName = "repository";

    public static void start() {
        DockerUtils.start();
        try {
            Connection connection = DriverManager.getConnection(connectionUrl);
            connection
                    .prepareStatement("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE")
                    .execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void stop() {
        DockerUtils.stop();
    }
}
