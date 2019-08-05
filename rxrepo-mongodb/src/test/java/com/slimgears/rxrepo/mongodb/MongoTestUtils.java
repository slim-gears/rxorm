package com.slimgears.rxrepo.mongodb;

import com.mongodb.ConnectionString;
import com.slimgears.rxrepo.mongodb.embed.MongoService;

class MongoTestUtils {
    final static int port = 27018;
    final static ConnectionString connectionString = new ConnectionString("mongodb://localhost:" + port);

    static AutoCloseable startMongo() {
        return MongoService.builder()
                .port(port)
                .enableReplica()
                .build()
                .start();
    }
}
