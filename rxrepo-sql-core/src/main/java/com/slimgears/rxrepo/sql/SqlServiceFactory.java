package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.query.provider.QueryProvider;

import java.util.function.Function;
import java.util.function.Supplier;

public interface SqlServiceFactory {
    SqlStatementProvider statementProvider();
    SqlStatementExecutor statementExecutor();
    SchemaProvider schemaProvider();
    SqlExpressionGenerator expressionGenerator();
    ReferenceResolver referenceResolver();
    QueryProvider queryProvider();

    static Builder builder() {
        return DefaultSqlServiceFactory.builder();
    }

    interface Builder {
        Builder statementProvider(Function<SqlServiceFactory, SqlStatementProvider> statementProvider);
        Builder statementExecutor(Function<SqlServiceFactory, SqlStatementExecutor> statementExecutor);
        Builder schemaProvider(Function<SqlServiceFactory, SchemaProvider> schemaProvider);
        Builder referenceResolver(Function<SqlServiceFactory, ReferenceResolver> referenceResolver);
        Builder expressionGenerator(Function<SqlServiceFactory, SqlExpressionGenerator> expressionGenerator);
        SqlServiceFactory build();

        default Builder statementProvider(Supplier<SqlStatementProvider> statementProvider) {
            return statementProvider(f -> statementProvider.get());
        }

        default Builder statementExecutor(Supplier<SqlStatementExecutor> statementExecutor) {
            return statementExecutor(f -> statementExecutor.get());
        }

        default Builder schemaProvider(Supplier<SchemaProvider> schemaProvider) {
            return schemaProvider(f -> schemaProvider.get());
        }

        default Builder referenceResolver(Supplier<ReferenceResolver> referenceResolver) {
            return referenceResolver(f -> referenceResolver.get());
        }

        default Builder expressionGenerator(Supplier<SqlExpressionGenerator> expressionGenerator) {
            return expressionGenerator(f -> expressionGenerator.get());
        }
    }
}
