package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.RepositoryConfigModel;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.SchedulingProvider;

import java.util.function.Function;
import java.util.function.Supplier;

import static com.slimgears.rxrepo.query.provider.QueryProvider.Decorator.of;

public interface SqlServiceFactory {
    SqlStatementProvider statementProvider();
    SqlStatementExecutor statementExecutor();
    SchemaGenerator schemaProvider();
    SqlExpressionGenerator expressionGenerator();
    ReferenceResolver referenceResolver();
    QueryProvider queryProvider();
    SchedulingProvider schedulingProvider();
    KeyEncoder keyEncoder();
    SqlTypeMapper typeMapper();
    Supplier<String> dbNameProvider();

    static Builder builder() {
        return DefaultSqlServiceFactory.builder();
    }

    abstract class Builder {
        private QueryProvider.Decorator decorator = QueryProvider.Decorator.identity();

        public abstract Builder statementProvider(Function<SqlServiceFactory, SqlStatementProvider> statementProvider);
        public abstract Builder statementExecutor(Function<SqlServiceFactory, SqlStatementExecutor> statementExecutor);
        public abstract Builder schemaProvider(Function<SqlServiceFactory, SchemaGenerator> schemaProvider);
        public abstract Builder referenceResolver(Function<SqlServiceFactory, ReferenceResolver> referenceResolver);
        public abstract Builder expressionGenerator(Function<SqlServiceFactory, SqlExpressionGenerator> expressionGenerator);
        public abstract Builder queryProviderGenerator(Function<SqlServiceFactory, QueryProvider> queryProviderGenerator);
        public abstract Builder schedulingProvider(Function<SqlServiceFactory, SchedulingProvider> executorPool);
        public abstract Builder keyEncoder(Function<SqlServiceFactory, KeyEncoder> keyEncoder);
        public abstract Builder typeMapper(Function<SqlServiceFactory, SqlTypeMapper> typeMapper);
        public abstract Builder dbNameProvider(Function<SqlServiceFactory, Supplier<String>> dbNameProvider);
        public abstract SqlServiceFactory build();

        public final Repository buildRepository(RepositoryConfigModel config, QueryProvider.Decorator... decorators) {
            return Repository.fromProvider(this.decorator.apply(build().queryProvider()), config, decorators);
        }

        public final SqlServiceFactory.Builder decorate(QueryProvider.Decorator... decorators) {
            this.decorator = this.decorator.andThen(of(decorators));
            return this;
        }

        public Builder keyEncoder(Supplier<KeyEncoder> keyEncoder) {
            return keyEncoder(f -> keyEncoder.get());
        }

        public Builder statementProvider(Supplier<SqlStatementProvider> statementProvider) {
            return statementProvider(f -> statementProvider.get());
        }

        public Builder statementExecutor(Supplier<SqlStatementExecutor> statementExecutor) {
            return statementExecutor(f -> statementExecutor.get());
        }

        public Builder schemaProvider(Supplier<SchemaGenerator> schemaProvider) {
            return schemaProvider(f -> schemaProvider.get());
        }

        public Builder referenceResolver(Supplier<ReferenceResolver> referenceResolver) {
            return referenceResolver(f -> referenceResolver.get());
        }

        public Builder expressionGenerator(Supplier<SqlExpressionGenerator> expressionGenerator) {
            return expressionGenerator(f -> expressionGenerator.get());
        }

        public Builder schedulingProvider(Supplier<SchedulingProvider> executorPool) {
            return schedulingProvider(f -> executorPool.get());
        }

        public Builder typeMapper(Supplier<SqlTypeMapper> typeMapper) {
            return typeMapper(f -> typeMapper.get());
        }

        public Builder dbName(String dbName) {
            return dbNameProvider(() -> dbName);
        }

        public Builder dbNameProvider(Supplier<String> dbNameProvider) {
            return dbNameProvider(f -> dbNameProvider);
        }
    }
}
