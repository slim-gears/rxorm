package com.slimgears.rxrepo.mongodb;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.decorator.LimitConcurrentOperationsQueryProviderDecorator;
import com.slimgears.rxrepo.query.decorator.LiveQueryProviderDecorator;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.generic.MoreStrings;

import java.time.Duration;

public class MongoRepository {
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int maxConcurrentRequests = defaultMaxConcurrentRequests();
        private String dbName = "repository";
        private String host = "localhost";
        private int port = 27017;
        private String user = null;
        private String password = null;
        private QueryProvider.Decorator decorator = QueryProvider.Decorator.identity();

        private Builder() {
        }

        public Builder maxConcurrentRequests(int maxConcurrentRequests) {
            this.maxConcurrentRequests = maxConcurrentRequests;
            return this;
        }

        public Builder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder decorate(QueryProvider.Decorator... decorators) {
            decorator = QueryProvider.Decorator.of(this.decorator, QueryProvider.Decorator.of(decorators));
            return this;
        }

        public Repository build() {
            String connectionString = createConnectionString();
            QueryProvider queryProvider = new MongoQueryProvider(connectionString, dbName, maxConcurrentRequests * 2);
            return Repository.fromProvider(queryProvider,
                    LiveQueryProviderDecorator.create(Duration.ofMillis(2000)),
                    decorator,
//                    UpdateReferencesFirstQueryProviderDecorator.create(),
                    LimitConcurrentOperationsQueryProviderDecorator.create(maxConcurrentRequests));
        }

        private String createConnectionString() {
            return user != null && password != null
                    ? MoreStrings.format("mongodb://{}:{}@{}:{}", user, password, host, port)
                    : MoreStrings.format("mongodb://{}:{}", host, port);
        }
    }

    private static int defaultMaxConcurrentRequests() {
        return Math.min(200, Math.max(16, Runtime.getRuntime().availableProcessors() * 8));
    }
}
