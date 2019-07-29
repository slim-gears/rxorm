package com.slimgears.rxrepo.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;
import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.store.IDownloadConfig;
import de.flapdoodle.embed.process.io.directories.FixedPath;
import de.flapdoodle.embed.process.io.progress.IProgressListener;
import io.reactivex.Completable;
import io.reactivex.Observable;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

class MongoTestUtils {
    private final static Logger log = LoggerFactory.getLogger(MongoTestUtils.class);
    final static int port = 27018;

    static ConnectionString connectionString = new ConnectionString("mongodb://localhost:" + port);

    static AutoCloseable startMongo() {
        try {
            IDownloadConfig downloadConfig = new DownloadConfigBuilder()
                    .defaults()
                    .defaultsForCommand(Command.MongoD)
                    .artifactStorePath(new FixedPath(".mongo/dist"))
                    .progressListener(new IProgressListener() {
                        private final AtomicInteger lastPercent = new AtomicInteger();

                        @Override
                        public void progress(String label, int percent) {
                            int roundPercent = Math.round(percent / 25) * 25;
                            if (lastPercent.get() != roundPercent) {
                                log.debug("{} download progress: {}% done", label, roundPercent);
                                lastPercent.set(roundPercent);
                            }
                        }

                        @Override
                        public void done(String label) {
                            log.info("Finished downloading {}", label);
                        }

                        @Override
                        public void start(String label) {
                            log.info("Starting download: {}", label);
                        }

                        @Override
                        public void info(String label, String message) {
                            log.info("{}: {}", label, message);
                        }
                    })
                    .build();

            IRuntimeConfig config = new RuntimeConfigBuilder()
                    .defaults(Command.MongoD)
                    .artifactStore(new ExtractedArtifactStoreBuilder()
                            .defaults(Command.MongoD)
                            .download(downloadConfig)
                            .tempDir(new FixedPath(Paths.get(".mongo/temp").toAbsolutePath().toString()))
                            .build())
                    .build();

            MongodStarter starter = MongodStarter.getInstance(config);

            IMongodConfig mongodConfig = new MongodConfigBuilder()
                    .version(Version.Main.PRODUCTION)
                    .net(new Net("localhost", port, false))
                    .replication(new Storage(null, "rs0", 5000))
                    .cmdOptions(new MongoCmdOptionsBuilder().useNoJournal(false).build())
                    .timeout(new Timeout(10000))
                    .stopTimeoutInMillis(10000)
                    .build();

            MongodExecutable executable = starter.prepare(mongodConfig);
            MongodProcess process = executable.start();

            MongoClient mongoClient = MongoClients.create(connectionString);
            MongoDatabase adminDb = mongoClient.getDatabase("admin");
            Document rsConfig = new Document("_id", "rs0")
                    .append("members", Collections.singletonList(
                            new Document("_id", 0)
                                    .append("host", "localhost:" + port)));

            Completable.fromPublisher(adminDb.runCommand(new Document("replSetInitiate", rsConfig)))
                    .blockingAwait();

            Observable.fromPublisher(adminDb.runCommand(new Document("replSetGetStatus", 1)))
                    .ignoreElements()
                    .blockingAwait();

            mongoClient.close();

            return () -> {
                System.out.println("Stopping mongod...");
                process.stop();
                System.out.println("Done");
            };

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
