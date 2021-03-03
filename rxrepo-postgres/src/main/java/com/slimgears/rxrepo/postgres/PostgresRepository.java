package com.slimgears.rxrepo.postgres;

import com.slimgears.rxrepo.query.RepositoryConfig;
import com.slimgears.rxrepo.sql.SqlServiceFactory;
import com.slimgears.rxrepo.sql.SqlStatementExecutor;
import com.slimgears.rxrepo.sql.jdbc.JdbcRepository;

import java.sql.Connection;
import java.util.function.Supplier;

public class PostgresRepository {
    public static class Builder<B extends Builder<B>> extends JdbcRepository.Builder<B> {
        private String schemaName = "repository";
        private SqlStatementExecutor.Decorator executorDecorator = SqlStatementExecutor.Decorator.identity();

        public B schemaName(String name) {
            this.schemaName = name;
            return self();
        }

        public B decorateExecutor(SqlStatementExecutor.Decorator... decorators) {
            executorDecorator = executorDecorator.andThen(SqlStatementExecutor.Decorator.of(decorators));
            return self();
        }

        @Override
        protected SqlServiceFactory.Builder<?> serviceFactoryBuilder(RepositoryConfig config, Supplier<Connection> connectionSupplier) {
            return super.serviceFactoryBuilder(config, connectionSupplier)
                    .dbName(schemaName)
                    .decorateExecutorBefore(() -> executorDecorator)
                    .statementProvider(sf -> new PostgresSqlStatementProvider(
                            sf.expressionGenerator(),
                            sf.typeMapper(),
                            sf.dbNameProvider()));
        }
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }
}
