package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import io.reactivex.Completable;
import io.reactivex.Scheduler;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public interface SqlServiceFactory {
    SqlStatementProvider statementProvider();
    SqlStatementExecutor statementExecutor();
    SchemaProvider schemaProvider();
    SqlExpressionGenerator expressionGenerator();
    SqlAssignmentGenerator assignmentGenerator();
    Scheduler scheduler();
    Completable shutdownSignal();
    ReferenceResolver referenceResolver();
    QueryProvider queryProvider();

    static Builder builder() {
        return DefaultSqlServiceFactory.builder();
    }

    abstract class Builder {
        public abstract Builder statementProvider(Function<SqlServiceFactory, SqlStatementProvider> statementProvider);
        public abstract Builder statementExecutor(Function<SqlServiceFactory, SqlStatementExecutor> statementExecutor);
        public abstract Builder schemaProvider(Function<SqlServiceFactory, SchemaProvider> schemaProvider);
        public abstract Builder referenceResolver(Function<SqlServiceFactory, ReferenceResolver> referenceResolver);
        public abstract Builder expressionGenerator(Function<SqlServiceFactory, SqlExpressionGenerator> expressionGenerator);
        public abstract Builder assignmentGenerator(Function<SqlServiceFactory, SqlAssignmentGenerator> assignmentGenerator);
        public abstract Builder scheduler(Scheduler scheduler);
        public abstract Builder shutdownSignal(Completable shutdown);
        public abstract SqlServiceFactory build();

        @SafeVarargs
        public final Repository buildRepository(UnaryOperator<QueryProvider>... decorators) {
            return Repository.fromProvider(build().queryProvider(), decorators);
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
    }
}
