package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.SchedulingProvider;
import com.slimgears.util.stream.Lazy;

import javax.annotation.Nonnull;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class DefaultSqlServiceFactory implements SqlServiceFactory {
    private final Lazy<SqlStatementProvider> statementProvider;
    private final Lazy<SqlStatementExecutor> statementExecutor;
    private final Lazy<ReferenceResolver> referenceResolver;
    private final Lazy<SchemaGenerator> schemaProvider;
    private final Lazy<SqlExpressionGenerator> expressionGenerator;
    private final Lazy<QueryProvider> queryProvider;
    private final Lazy<SchedulingProvider> executorPool;
    private final Lazy<KeyEncoder> keyEncoder;
    private final Lazy<SqlTypeMapper> typeMapper;
    private final Lazy<Supplier<String>> dbNameProvider;

    private DefaultSqlServiceFactory(
            @Nonnull Function<SqlServiceFactory, SqlStatementProvider> statementProvider,
            @Nonnull Function<SqlServiceFactory, SqlStatementExecutor> statementExecutor,
            @Nonnull Function<SqlServiceFactory, ReferenceResolver> referenceResolver,
            @Nonnull Function<SqlServiceFactory, SchemaGenerator> schemaProvider,
            @Nonnull Function<SqlServiceFactory, SqlExpressionGenerator> expressionGenerator,
            @Nonnull Function<SqlServiceFactory, QueryProvider> queryProviderGenerator,
            @Nonnull Function<SqlServiceFactory, SchedulingProvider> executorPool,
            @Nonnull Function<SqlServiceFactory, KeyEncoder> keyEncoder,
            @Nonnull Function<SqlServiceFactory, SqlTypeMapper> typeMapper,
            @Nonnull Function<SqlServiceFactory, Supplier<String>> dbNameProvider) {
        this.statementProvider = Lazy.of(() -> statementProvider.apply(this));
        this.statementExecutor = Lazy.of(() -> statementExecutor.apply(this));
        this.referenceResolver = Lazy.of(() -> referenceResolver.apply(this));
        this.schemaProvider = Lazy.of(() -> CacheSchemaGeneratorDecorator.decorate(schemaProvider.apply(this)));
        this.expressionGenerator = Lazy.of(() -> expressionGenerator.apply(this));
        this.queryProvider = Lazy.of(() -> queryProviderGenerator.apply(this));
        this.executorPool = Lazy.of(() -> executorPool.apply(this));
        this.keyEncoder = Lazy.of(() -> keyEncoder.apply(this));
        this.typeMapper = Lazy.of(() -> typeMapper.apply(this));
        this.dbNameProvider = Lazy.of(() -> dbNameProvider.apply(this));
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
    public SchemaGenerator schemaProvider() {
        return schemaProvider.get();
    }

    @Override
    public SqlExpressionGenerator expressionGenerator() {
        return expressionGenerator.get();
    }

    @Override
    public ReferenceResolver referenceResolver() {
        return referenceResolver.get();
    }

    @Override
    public QueryProvider queryProvider() {
        return queryProvider.get();
    }

    @Override
    public SchedulingProvider schedulingProvider() {
        return executorPool.get();
    }

    @Override
    public KeyEncoder keyEncoder() {
        return keyEncoder.get();
    }

    @Override
    public SqlTypeMapper typeMapper() {
        return typeMapper.get();
    }

    @Override
    public Supplier<String> dbNameProvider() {
        return dbNameProvider.get();
    }

    public static Builder builder() {
        return new Builder()
                .expressionGenerator(factory -> new DefaultSqlExpressionGenerator())
                .statementProvider(factory -> new DefaultSqlStatementProvider(
                        factory.expressionGenerator(),
                        factory.typeMapper(),
                        factory.dbNameProvider()));
    }

    public static class Builder extends SqlServiceFactory.Builder<Builder> {
        @Override
        public SqlServiceFactory build() {
            Function<SqlServiceFactory, SqlStatementExecutor> origExecutor = requireNonNull(statementExecutor);
            Function<SqlServiceFactory, SqlStatementExecutor> decoratedExecutor = sf -> origExecutor.apply(sf).decorate(executorDecorator.apply(sf));

            return new DefaultSqlServiceFactory(
                    requireNonNull(statementProvider),
                    requireNonNull(decoratedExecutor),
                    requireNonNull(referenceResolver),
                    requireNonNull(schemaProvider),
                    requireNonNull(expressionGenerator),
                    requireNonNull(queryProviderGenerator),
                    requireNonNull(schedulingProvider),
                    requireNonNull(keyEncoder),
                    requireNonNull(typeMapper),
                    requireNonNull(dbNameProvider));
        }
    }
}
