package com.slimgears.rxrepo.orientdb;

import com.google.common.collect.ImmutableMap;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.RepositoryConfig;
import com.slimgears.rxrepo.query.RepositoryConfigModelBuilder;
import com.slimgears.rxrepo.query.decorator.LiveQueryProviderDecorator;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.sql.DefaultSqlStatementProvider;
import com.slimgears.rxrepo.sql.SqlServiceFactory;
import com.slimgears.util.stream.Lazy;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.internal.functions.Functions;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.CompletableSubject;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class OrientDbRepository {
    public enum Type {
        Memory,
        Persistent
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements RepositoryConfigModelBuilder<Builder> {

        private final static Object lock = new Object();
        private final static ImmutableMap<Type, ODatabaseType> dbTypeMap = ImmutableMap
                .<Type, ODatabaseType>builder()
                .put(Type.Memory, ODatabaseType.MEMORY)
                .put(Type.Persistent, ODatabaseType.PLOCAL)
                .build();
        private String url = "embedded:repository";

        private String dbName = "repository";
        private ODatabaseType dbType = ODatabaseType.MEMORY;
        private String user = "admin";
        private String password = "admin";
        private Scheduler scheduler = Schedulers.io();
        private QueryProvider.Decorator decorator = QueryProvider.Decorator.identity();
        private RepositoryConfig.Builder configBuilder = RepositoryConfig
                .builder()
                .retryCount(10)
                .retryInitialDurationMillis(10)
                .debounceTimeoutMillis(100);
        public final Builder url(@Nonnull String url) {
            this.url = url;
            return this;
        }

        public final Builder type(@Nonnull Type type) {
            this.dbType = dbTypeMap.get(type);
            return this;
        }

        public final Builder name(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public final Builder user(@Nonnull String user) {
            this.user = user;
            return this;
        }

        public final Builder password(@Nonnull String password) {
            this.password = password;
            return this;
        }

        public final Builder scheduler(@Nonnull Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public final Builder decorate(@Nonnull QueryProvider.Decorator... decorators) {
            this.decorator = this.decorator.andThen(QueryProvider.Decorator.of(decorators));
            return this;
        }

        public final Repository build() {
            Objects.requireNonNull(url);
            Objects.requireNonNull(dbName);
            Objects.requireNonNull(dbType);
            Objects.requireNonNull(user);
            Objects.requireNonNull(password);

            Lazy<OrientDB> dbClient = Lazy.of(() -> createClient(url, dbName, dbType));
            Map<ODatabaseDocument, CompletableSubject> sessions = new ConcurrentHashMap<>();
            CompletableSubject shutdownSubject = CompletableSubject.create();

            return serviceFactoryBuilder(
                    () -> {
                        ODatabaseDocument session = createSession(dbClient, dbName, user, password);
                        sessions.put(session, CompletableSubject.create());
                        return session;
                    },
                    session -> Optional.ofNullable(sessions.remove(session))
                            .ifPresent(CompletableSubject::onComplete))
                    .shutdownSignal(shutdownSubject)
                    .scheduler(scheduler)
                    .decorate(decorator)
                    .decorate(OrientDbDropDatabaseQueryProviderDecorator.create(dbClient, dbName))
                    .buildRepository(configBuilder.build())
                    .onClose(repo -> {
                        shutdownSubject.onComplete();
                        if (!Observable.fromIterable(sessions.values())
                                .flatMapCompletable(Functions.identity())
                                .blockingAwait(4, TimeUnit.SECONDS)) {
                            sessions.keySet().forEach(ODatabaseDocument::close);
                        }
                        dbClient.close();
                    });
        }

        private static OrientDB createClient(String url, String dbName, ODatabaseType dbType) {
            OrientDB client = new OrientDB(url, OrientDBConfig.defaultConfig());
            if (!client.exists(dbName)) {
                synchronized (lock) {
                    client.createIfNotExists(dbName, dbType);
                }
            }
            return client;
        }

        private static ODatabaseDocument createSession(Supplier<OrientDB> clientSupplier, String dbName, String user, String password) {
            return clientSupplier.get().open(dbName, user, password);
        }

        @Override
        public Builder retryCount(int value) {
            configBuilder.retryCount(value);
            return this;
        }

        @Override
        public Builder debounceTimeoutMillis(int value) {
            configBuilder.debounceTimeoutMillis(value);
            return this;
        }

        @Override
        public Builder retryInitialDurationMillis(int value) {
            configBuilder.retryInitialDurationMillis(value);
            return this;
        }
    }

    private static SqlServiceFactory.Builder serviceFactoryBuilder(Supplier<ODatabaseDocument> sessionProvider, Consumer<ODatabaseDocument> sessionCloser) {
        OrientDbSessionProvider dbSessionProvider = OrientDbSessionProvider.create(sessionProvider, sessionCloser);
        return SqlServiceFactory.builder()
                .schemaProvider(svc -> new OrientDbSchemaProvider(dbSessionProvider))
                .statementExecutor(svc -> new OrientDbStatementExecutor(dbSessionProvider, svc.scheduler(), svc.shutdownSignal()))
                .expressionGenerator(OrientDbSqlExpressionGenerator::new)
                .assignmentGenerator(svc -> new OrientDbAssignmentGenerator(svc.expressionGenerator()))
                .statementProvider(svc -> new DefaultSqlStatementProvider(svc.expressionGenerator(), svc.assignmentGenerator(), svc.schemaProvider()))
                .referenceResolver(svc -> new OrientDbReferenceResolver(svc.statementProvider()))
                .decorate(LiveQueryProviderDecorator.decorator());
    }
}
