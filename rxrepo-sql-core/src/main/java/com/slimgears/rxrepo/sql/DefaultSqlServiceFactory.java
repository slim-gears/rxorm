package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.stream.Lazy;
import io.reactivex.Completable;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

import javax.annotation.Nonnull;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class DefaultSqlServiceFactory implements SqlServiceFactory {
    private final Lazy<SqlStatementProvider> statementProvider;
    private final Lazy<SqlStatementExecutor> statementExecutor;
    private final Lazy<ReferenceResolver> referenceResolver;
    private final Lazy<SchemaProvider> schemaProvider;
    private final Lazy<SqlExpressionGenerator> expressionGenerator;
    private final Lazy<SqlAssignmentGenerator> assignmentGenerator;
    private final Lazy<QueryProvider> queryProvider;
    private final Scheduler scheduler;
    private final Completable shutdownSignal;

    private DefaultSqlServiceFactory(
            @Nonnull Function<SqlServiceFactory, SqlStatementProvider> statementProvider,
            @Nonnull Function<SqlServiceFactory, SqlStatementExecutor> statementExecutor,
            @Nonnull Function<SqlServiceFactory, ReferenceResolver> referenceResolver,
            @Nonnull Function<SqlServiceFactory, SchemaProvider> schemaProvider,
            @Nonnull Function<SqlServiceFactory, SqlExpressionGenerator> expressionGenerator,
            @Nonnull Function<SqlServiceFactory, SqlAssignmentGenerator> assignmentGenerator,
            @Nonnull Scheduler scheduler,
            @Nonnull Completable shutdownSignal) {

        this.statementProvider = Lazy.of(() -> statementProvider.apply(this));
        this.statementExecutor = Lazy.of(() -> statementExecutor.apply(this));
        this.referenceResolver = Lazy.of(() -> referenceResolver.apply(this));
        this.schemaProvider = Lazy.of(() -> CacheSchemaProviderDecorator.decorate(schemaProvider.apply(this)));
        this.expressionGenerator = Lazy.of(() -> expressionGenerator.apply(this));
        this.assignmentGenerator = Lazy.of(() -> assignmentGenerator.apply(this));
        this.scheduler = scheduler;
        this.shutdownSignal = shutdownSignal;
        this.queryProvider = Lazy.of(() -> new SqlQueryProvider(
                statementProvider(),
                statementExecutor(),
                expressionGenerator(),
                schemaProvider(),
                referenceResolver()));
    }

    @Override
    public SqlStatementProvider statementProvider() {
        return this.statementProvider.get();
    }

    @Override
    public SqlStatementExecutor statementExecutor() {
        return this.statementExecutor.get();
    }

    @Override
    public SchemaProvider schemaProvider() {
        return schemaProvider.get();
    }

    @Override
    public SqlExpressionGenerator expressionGenerator() {
        return expressionGenerator.get();
    }

    @Override
    public SqlAssignmentGenerator assignmentGenerator() {
        return assignmentGenerator.get();
    }

    @Override
    public Scheduler scheduler() {
        return scheduler;
    }

    @Override
    public Completable shutdownSignal() {
        return shutdownSignal;
    }

    @Override
    public ReferenceResolver referenceResolver() {
        return referenceResolver.get();
    }

    @Override
    public QueryProvider queryProvider() {
        return queryProvider.get();
    }

    public static SqlServiceFactory.Builder builder() {
        return new Builder()
                .expressionGenerator(DefaultSqlExpressionGenerator::new)
                .statementProvider(factory -> new DefaultSqlStatementProvider(
                        factory.expressionGenerator(),
                        factory.assignmentGenerator(),
                        factory.schemaProvider()));
    }

    static class Builder extends SqlServiceFactory.Builder {
        private Function<SqlServiceFactory, SqlStatementProvider> statementProvider;
        private Function<SqlServiceFactory, SqlStatementExecutor> statementExecutor;
        private Function<SqlServiceFactory, SchemaProvider> schemaProvider;
        private Function<SqlServiceFactory, ReferenceResolver> referenceResolver;
        private Function<SqlServiceFactory, SqlExpressionGenerator> expressionGenerator;
        private Function<SqlServiceFactory, SqlAssignmentGenerator> assignmentGenerator;
        private Scheduler scheduler = Schedulers.single();
        private Completable shutdownSignal = Completable.never();

        @Override
        public SqlServiceFactory.Builder statementProvider(Function<SqlServiceFactory, SqlStatementProvider> statementProvider) {
            this.statementProvider = statementProvider;
            return this;
        }

        @Override
        public SqlServiceFactory.Builder statementExecutor(Function<SqlServiceFactory, SqlStatementExecutor> statementExecutor) {
            this.statementExecutor = statementExecutor;
            return this;
        }

        @Override
        public SqlServiceFactory.Builder schemaProvider(Function<SqlServiceFactory, SchemaProvider> schemaProvider) {
            this.schemaProvider = schemaProvider;
            return this;
        }

        @Override
        public SqlServiceFactory.Builder referenceResolver(Function<SqlServiceFactory, ReferenceResolver> referenceResolver) {
            this.referenceResolver = referenceResolver;
            return this;
        }

        @Override
        public SqlServiceFactory.Builder expressionGenerator(Function<SqlServiceFactory, SqlExpressionGenerator> expressionGenerator) {
            this.expressionGenerator = expressionGenerator;
            return this;
        }

        @Override
        public SqlServiceFactory.Builder assignmentGenerator(Function<SqlServiceFactory, SqlAssignmentGenerator> assignmentGenerator) {
            this.assignmentGenerator = assignmentGenerator;
            return this;
        }

        @Override
        public SqlServiceFactory.Builder scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        @Override
        public SqlServiceFactory.Builder shutdownSignal(Completable shutdown) {
            this.shutdownSignal = shutdown;
            return this;
        }

        @Override
        public SqlServiceFactory build() {
            return new DefaultSqlServiceFactory(
                    requireNonNull(statementProvider),
                    requireNonNull(statementExecutor),
                    requireNonNull(referenceResolver),
                    requireNonNull(schemaProvider),
                    requireNonNull(expressionGenerator),
                    requireNonNull(assignmentGenerator),
                    scheduler,
                    shutdownSignal);
        }
    }
}
