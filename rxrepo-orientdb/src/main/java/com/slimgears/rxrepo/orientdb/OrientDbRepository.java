package com.slimgears.rxrepo.orientdb;

import com.google.common.collect.ImmutableMap;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.RepositoryConfig;
import com.slimgears.rxrepo.query.RepositoryConfigModelBuilder;
import com.slimgears.rxrepo.query.decorator.CacheQueryProviderDecorator;
import com.slimgears.rxrepo.query.decorator.LiveQueryProviderDecorator;
import com.slimgears.rxrepo.query.decorator.RecursiveLiveQueryProviderDecorator;
import com.slimgears.rxrepo.query.decorator.UpdateReferencesFirstQueryProviderDecorator;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.sql.DefaultSqlStatementProvider;
import com.slimgears.rxrepo.sql.SqlQueryProvider;
import com.slimgears.rxrepo.sql.SqlServiceFactory;
import com.slimgears.util.stream.Lazy;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class OrientDbRepository {
    public enum Type {
        Memory,
        Persistent
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements RepositoryConfigModelBuilder<Builder> {
        private final static int pageSize = 64 * 1024;
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
        private String serverUser = "root";
        private String serverPassword = "root";
        private boolean batchSupport = false;
        private int maxNotificationQueues = 10;
        private QueryProvider.Decorator decorator = QueryProvider.Decorator.identity();
        private RepositoryConfig.Builder configBuilder = RepositoryConfig
                .builder()
                .retryCount(10)
                .retryInitialDurationMillis(10)
                .debounceTimeoutMillis(100);
        private final Map<OGlobalConfiguration, Object> customConfig = new HashMap<>();

        public final Builder enableBatchSupport() {
            return enableBatchSupport(true);
        }

        public final Builder enableBatchSupport(boolean enable) {
            this.batchSupport = enable;
            return this;
        }

        public final Builder maxNotificationQueues(int maxNotificationQueues) {
            this.maxNotificationQueues = maxNotificationQueues;
            return this;
        }

        public final Builder maxConnections(int maxConnections) {
            this.customConfig.put(OGlobalConfiguration.DB_POOL_MAX, maxConnections);
            return this;
        }

        public final Builder maxNonHeapMemory(int maxNonHeapMemoryBytes) {
            this.customConfig.put(OGlobalConfiguration.DIRECT_MEMORY_POOL_LIMIT, maxNonHeapMemoryBytes / pageSize);
            return this;
        }

        public final Builder setProperty(String key, Object value) {
            customConfig.put(OGlobalConfiguration.findByKey(key), value);
            return this;
        }

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

        public final Builder serverUser(@Nonnull String serverUser) {
            this.serverUser = serverUser;
            return this;
        }

        public final Builder serverPassword(@Nonnull String serverPassword) {
            this.serverPassword = serverPassword;
            return this;
        }

        public final Builder decorate(@Nonnull QueryProvider.Decorator... decorators) {
            this.decorator = this.decorator.andThen(QueryProvider.Decorator.of(decorators));
            return this;
        }

        public final Repository build() {
            Objects.requireNonNull(url);
            Objects.requireNonNull(serverUser);
            Objects.requireNonNull(serverPassword);
            Objects.requireNonNull(dbName);
            Objects.requireNonNull(dbType);
            Objects.requireNonNull(user);
            Objects.requireNonNull(password);

            Lazy<OrientDB> dbClient = Lazy.of(() -> createClient(url, serverUser, serverPassword, dbName, dbType));
            Lazy<ODatabasePool> dbPool = Lazy.of(() -> new ODatabasePool(dbClient.get(), dbName, user, password));

            OrientDbSessionProvider dbSessionProvider = OrientDbSessionProvider.create(
                    () -> dbPool.get().acquire(),
                    ODatabase::close);

            return serviceFactoryBuilder(dbSessionProvider)
                    .decorate(
                            CacheQueryProviderDecorator.create(),
                            RecursiveLiveQueryProviderDecorator.create(),
                            LiveQueryProviderDecorator.create(),
                            batchSupport ? OrientDbUpdateReferencesFirstQueryProviderDecorator.create() : UpdateReferencesFirstQueryProviderDecorator.create(),
                            OrientDbDropDatabaseQueryProviderDecorator.create(dbClient, dbName),
                            decorator)
                    .buildRepository(configBuilder.build())
                    .onClose(repo -> dbPool.close());
        }

        private OrientDB createClient(String url, String serverUser, String serverPassword, String dbName, ODatabaseType dbType) {
            OrientDBConfig config = OrientDBConfig.builder()
                    .fromGlobalMap(customConfig)
                    .build();

            OrientDB client = new OrientDB(url, serverUser, serverPassword, config);
            if (!client.exists(dbName)) {
                synchronized (lock) {
                    client.createIfNotExists(dbName, dbType);
                }
            }
            return client;
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

        private SqlServiceFactory.Builder serviceFactoryBuilder(OrientDbSessionProvider dbSessionProvider) {
            return SqlServiceFactory.builder()
                    .schemaProvider(svc -> new OrientDbSchemaProvider(dbSessionProvider))
                    .statementExecutor(svc -> OrientDbMappingStatementExecutor.decorate(new OrientDbStatementExecutor(dbSessionProvider)))
                    .expressionGenerator(OrientDbSqlExpressionGenerator::new)
                    .assignmentGenerator(svc -> new OrientDbAssignmentGenerator(svc.expressionGenerator()))
                    .statementProvider(svc -> new DefaultSqlStatementProvider(svc.expressionGenerator(), svc.assignmentGenerator(), svc.schemaProvider()))
                    .referenceResolver(svc -> new OrientDbReferenceResolver(svc.statementProvider()))
                    .queryProviderGenerator(svc -> batchSupport ? OrientDbQueryProvider.create(svc, dbSessionProvider, maxNotificationQueues) : SqlQueryProvider.create(svc, maxNotificationQueues));
        }
    }
}
