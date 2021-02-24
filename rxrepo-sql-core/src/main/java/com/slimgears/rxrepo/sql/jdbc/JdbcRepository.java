package com.slimgears.rxrepo.sql.jdbc;

import com.slimgears.rxrepo.query.RepositoryConfig;
import com.slimgears.rxrepo.query.decorator.BatchUpdateQueryProviderDecorator;
import com.slimgears.rxrepo.query.decorator.LockQueryProviderDecorator;
import com.slimgears.rxrepo.query.decorator.ObserveOnSchedulingQueryProviderDecorator;
import com.slimgears.rxrepo.query.decorator.UpdateReferencesFirstQueryProviderDecorator;
import com.slimgears.rxrepo.sql.*;
import com.slimgears.rxrepo.util.SemaphoreLockProvider;
import com.slimgears.util.stream.Safe;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Objects;
import java.util.function.Supplier;

public class JdbcRepository {
    public static class Builder<B extends Builder<B>> extends AbstractSqlRepositoryBuilder<B> {
        private Supplier<Connection> connectionSupplier;
        private int batchSize = 0;

        public B connection(Supplier<Connection> connectionSupplier) {
            this.connectionSupplier = connectionSupplier;
            return self();
        }

        public B connection(Connection connection) {
            return connection(() -> connection);
        }

        public B connection(String connectionStr) {
            return connection(Safe.ofSupplier(() -> DriverManager.getConnection(connectionStr)));
        }

        public B enableBatch(int batchSize) {
            this.batchSize = batchSize;
            return self();
        }

        @Override
        protected SqlServiceFactory.Builder serviceFactoryBuilder(RepositoryConfig config) {
            Objects.requireNonNull(connectionSupplier);
            return serviceFactoryBuilder(config, connectionSupplier);
        }

        protected SqlServiceFactory.Builder serviceFactoryBuilder(RepositoryConfig config, Supplier<Connection> connectionSupplier) {
            return SqlServiceFactory.builder()
                    .keyEncoder(DigestKeyEncoder::create)
                    .expressionGenerator(sf -> new DefaultSqlExpressionGenerator(sf.keyEncoder(), sf.typeMapper()))
                    .dbNameProvider(() -> "repository")
                    .typeMapper(() -> SqlTypes.instance)
                    .schemaProvider(sf -> new JdbcSchemaGenerator(sf.statementExecutor(), sf.statementProvider()))
                    .referenceResolver(sf -> new DefaultSqlReferenceResolver(sf.keyEncoder(), sf.expressionGenerator()))
                    .statementProvider(sf -> new DefaultSqlStatementProvider(sf.expressionGenerator(), sf.typeMapper(), sf.dbNameProvider()))
                    .statementExecutor(sf -> new JdbcSqlStatementExecutor(connectionSupplier.get(), sf.typeMapper()))
                    .schedulingProvider(schedulingProvider)
                    .decorate(
                            LockQueryProviderDecorator.create(SemaphoreLockProvider.create()),
//                            LiveQueryProviderDecorator.create(Duration.ofMillis(config.aggregationDebounceTimeMillis())),
                            ObserveOnSchedulingQueryProviderDecorator.create(schedulingProvider.get()),
                            UpdateReferencesFirstQueryProviderDecorator.create(),
                            BatchUpdateQueryProviderDecorator.create(batchSize)
                    );
        }
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }
}
