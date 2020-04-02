package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.RepositoryConfigModel;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.LockProvider;
import com.slimgears.rxrepo.util.SchedulingProvider;

import java.util.function.Function;
import java.util.function.Supplier;

import static com.slimgears.rxrepo.query.provider.QueryProvider.Decorator.of;

public interface SqlServiceFactory {
    SqlStatementProvider statementProvider();
    SqlStatementExecutor statementExecutor();
    SchemaProvider schemaProvider();
    SqlExpressionGenerator expressionGenerator();
    SqlAssignmentGenerator assignmentGenerator();
    ReferenceResolver referenceResolver();
    QueryProvider queryProvider();
    SchedulingProvider schedulingProvider();

    static Builder builder() {
        return DefaultSqlServiceFactory.builder();
    }

    abstract class Builder {
        private QueryProvider.Decorator decorator = QueryProvider.Decorator.identity();

        public abstract Builder statementProvider(Function<SqlServiceFactory, SqlStatementProvider> statementProvider);
        public abstract Builder statementExecutor(Function<SqlServiceFactory, SqlStatementExecutor> statementExecutor);
        public abstract Builder schemaProvider(Function<SqlServiceFactory, SchemaProvider> schemaProvider);
        public abstract Builder referenceResolver(Function<SqlServiceFactory, ReferenceResolver> referenceResolver);
        public abstract Builder expressionGenerator(Function<SqlServiceFactory, SqlExpressionGenerator> expressionGenerator);
        public abstract Builder assignmentGenerator(Function<SqlServiceFactory, SqlAssignmentGenerator> assignmentGenerator);
        public abstract Builder queryProviderGenerator(Function<SqlServiceFactory, QueryProvider> queryProviderGenerator);
        public abstract Builder schedulingProvider(Function<SqlServiceFactory, SchedulingProvider> executorPool);
        public abstract SqlServiceFactory build();

        public final Repository buildRepository(RepositoryConfigModel config, QueryProvider.Decorator... decorators) {
            return Repository.fromProvider(this.decorator.apply(build().queryProvider()), config, decorators);
        }

        public final SqlServiceFactory.Builder decorate(QueryProvider.Decorator... decorators) {
            this.decorator = this.decorator.andThen(of(decorators));
            return this;
        }

        public Builder statementProvider(Supplier<SqlStatementProvider> statementProvider) {
            return statementProvider(f -> statementProvider.get());
        }

        public Builder statementExecutor(Supplier<SqlStatementExecutor> statementExecutor) {
            return statementExecutor(f -> statementExecutor.get());
        }

        public Builder schemaProvider(Supplier<SchemaProvider> schemaProvider) {
            return schemaProvider(f -> schemaProvider.get());
        }

        public Builder referenceResolver(Supplier<ReferenceResolver> referenceResolver) {
            return referenceResolver(f -> referenceResolver.get());
        }

        public Builder expressionGenerator(Supplier<SqlExpressionGenerator> expressionGenerator) {
            return expressionGenerator(f -> expressionGenerator.get());
        }

        public Builder assignmentGenerator(Supplier<SqlAssignmentGenerator> assignmentGenerator) {
            return assignmentGenerator(f -> assignmentGenerator.get());
        }

        public Builder schedulingProvider(Supplier<SchedulingProvider> executorPool) {
            return schedulingProvider(f -> executorPool.get());
        }
    }
}
