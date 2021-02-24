package com.slimgears.rxrepo.sql;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.CollectionExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.expressions.internal.MoreTypeTokens;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.*;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.rxrepo.util.PropertyResolvers;
import com.slimgears.rxrepo.util.SchedulingProvider;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;
import io.reactivex.*;
import io.reactivex.functions.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.slimgears.util.generic.LazyString.lazy;

@SuppressWarnings("UnstableApiUsage")
public class DefaultSqlQueryProvider implements QueryProvider {
    private final static Logger log = LoggerFactory.getLogger(DefaultSqlQueryProvider.class);
    protected final SqlStatementProvider statementProvider;
    private final SqlStatementExecutor statementExecutor;
    protected final SchemaGenerator schemaGenerator;
    private final ReferenceResolver referenceResolver;
    private final SchedulingProvider schedulingProvider;
    private final Map<SqlStatement, Observable<Notification<PropertyResolver>>> liveQueriesCache = new ConcurrentHashMap<>();

    protected DefaultSqlQueryProvider(SqlStatementProvider statementProvider,
                                      SqlStatementExecutor statementExecutor,
                                      SchemaGenerator schemaGenerator,
                                      ReferenceResolver referenceResolver,
                                      SchedulingProvider schedulingProvider) {
        this.statementProvider = statementProvider;
        this.statementExecutor = statementExecutor;
        this.schemaGenerator = schemaGenerator;
        this.referenceResolver = referenceResolver;
        this.schedulingProvider = schedulingProvider;
    }

    public static QueryProvider create(SqlServiceFactory serviceFactory) {
        return new DefaultSqlQueryProvider(
                serviceFactory.statementProvider(),
                serviceFactory.statementExecutor(),
                serviceFactory.schemaProvider(),
                serviceFactory.referenceResolver(),
                serviceFactory.schedulingProvider());
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return Optional
                .of(entities)
                .filter(e -> !Iterables.isEmpty(e))
                .map(meta -> schemaGenerator.useTable(metaClass)
                        .doOnSubscribe(d -> log.trace("Beginning creating class {}", lazy(metaClass::simpleName)))
                        .doOnComplete(() -> log.trace("Finished creating class {}", lazy(metaClass::simpleName)))
                        .andThen(statementExecutor.executeCommand(
                                statementProvider.forInsert(
                                        metaClass,
                                        Streams.fromIterable(entities)
                                                .map(e -> PropertyResolver.fromObject(metaClass, e))
                                                .collect(Collectors.toList()),
                                        referenceResolver))))
                .orElseGet(Completable::complete);
    }

    @Override
    public <K, S> Single<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity, boolean recursive) {
        return insertOrUpdate(metaClass, PropertyResolver.fromObject(metaClass, entity));
    }

    @Override
    public <K, S> Maybe<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, boolean recursive, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        SqlStatement statement = statementProvider.forQuery(QueryInfo
                .<K, S, S>builder()
                .metaClass(metaClass)
                .predicate(PropertyExpression.ofObject(metaClass.keyProperty()).eq(key))
                .limit(1L)
                .build());

        return schemaGenerator.useTable(metaClass)
            .andThen(statementExecutor
                .executeQuery(statement)
                .firstElement()
                .flatMap((PropertyResolver pr) -> {
                    S oldObj = pr.toObject(metaClass);
                    return entityUpdater
                            .apply(Maybe.just(oldObj))
                            .map(newObj -> pr.mergeWith(PropertyResolver.fromObject(metaClass, newObj)))
                            .filter(newPr -> !pr.equals(newPr))
                            .flatMap(newPr -> update(metaClass, newPr).toMaybe());
                })
                .switchIfEmpty(Maybe.defer(() -> entityUpdater
                        .apply(Maybe.empty())
                        .flatMap(e -> insert(metaClass, e).toMaybe()))));
    }

    private <K, S> Single<Supplier<S>> update(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver) {
        SqlStatement statement = statementProvider.forUpdate(metaClass, propertyResolver, referenceResolver);
        return insertOrUpdate(metaClass, statement);
    }

    private <K, S> Single<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver) {
        SqlStatement statement = statementProvider.forInsertOrUpdate(metaClass, propertyResolver, referenceResolver);
        return insertOrUpdate(metaClass, statement);
    }

    private <K, S> Single<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, SqlStatement statement) {
        return schemaGenerator.useTable(metaClass)
                .doOnSubscribe(d -> log.trace("Ensuring class {}", metaClass.simpleName()))
                .doOnError(e -> log.trace("Error when updating class: {}", metaClass.simpleName(), e))
                .doOnComplete(() -> log.trace("Class updated {}", metaClass.simpleName()))
                .andThen(statementExecutor
                        .executeCommandReturnEntries(statement)
                        .<Supplier<S>>map(pr -> () -> pr.toObject(metaClass))
                        .doOnSubscribe(d -> log.trace("Executing statement: {}", statement.statement()))
                        .doOnError(e -> log.trace("Failed to execute statement: {}", statement.statement(), e))
                        .doOnComplete(() -> log.trace("Execution complete: {}", statement.statement()))
                        .doOnNext(obj -> log.trace("Updated {}", obj))
                        .take(1)
                        .singleOrError());
    }

    private <K, S> Single<Supplier<S>> insert(MetaClassWithKey<K, S> metaClass, S entity) {
        SqlStatement statement = statementProvider.forInsert(metaClass, entity, referenceResolver);
        return insertOrUpdate(metaClass, statement);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> query(QueryInfo<K, S, T> query) {
        log.trace("Preparing query of {}", query.metaClass().simpleName());
        Scheduler scheduler = schedulingProvider.scheduler();
        TypeToken<? extends T> objectType = HasMapping.objectType(query);
        return schemaGenerator
                .useTable(query.metaClass())
                .andThen(statementExecutor
                        .executeQuery(statementProvider.forQuery(query))
                        .compose(toCreateNotifications(objectType, query.mapping(), query.properties())))
                .observeOn(scheduler);
    }

    @SuppressWarnings("unchecked")
    private <T> ObservableTransformer<PropertyResolver, Notification<T>> toCreateNotifications(TypeToken<? extends T> objectType,
                                                                     ObjectExpression<?, T> mapping,
                                                                     ImmutableSet<PropertyExpression<T, ?, ?>> properties) {
        Function<PropertyResolver, Maybe<Notification<T>>> mapper = Optional
                .ofNullable(mapping)
                .flatMap(Optionals.ofType(PropertyExpression.class))
                .map(PropertyExpression::path)
                .<Function<PropertyResolver, Maybe<Notification<T>>>>map(path -> pr -> Optional
                            .ofNullable(pr.getProperty(path, objectType))
                            .map(obj -> obj instanceof PropertyResolver
                                    ? PropertyResolvers.withProperties(properties, () -> (PropertyResolver) obj).toObject(objectType)
                                    : (T)obj)
                            .map(obj -> Notification.ofCreated(obj, generationOf(pr)))
                            .map(Maybe::just)
                            .orElseGet(Maybe::empty))
                .orElse(pr -> Maybe
                        .fromCallable(() -> PropertyResolvers.withProperties(properties, () -> pr.toObject(objectType)))
                        .map(obj -> Notification.ofCreated(obj, generationOf(pr))));
        return src -> src.flatMapMaybe(mapper);
    }

    private Long generationOf(PropertyResolver propertyResolver) {
        return Optional.ofNullable(propertyResolver.getProperty(SqlFields.sequenceFieldName, TypeToken.of(Long.class)))
                .map(Long.class::cast)
                .orElse(0L);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        log.trace("Preparing live query of {}", query.metaClass().simpleName());
        TypeToken<? extends T> objectType = HasMapping.objectType(query);
        Scheduler scheduler = schedulingProvider.scheduler();
        SqlStatement statement = statementProvider.forQuery(query.toBuilder().properties(ImmutableSet.of()).build());
        return schemaGenerator
                .useTable(query.metaClass())
                .andThen(liveQueryForStatement(statement))
                .map(notification -> notification.<T>map(pr -> PropertyResolvers.withProperties(query.properties(), () -> pr.toObject(objectType))))
                .observeOn(scheduler)
                .doOnNext(n -> log.trace("{}: {} {}",
                        query.metaClass().simpleName(),
                        n.isCreate() ? "Create" : n.isModify() ? "Modify" : n.isDelete() ? "Delete" : "Empty",
                        n.sequenceNumber()));
    }

    private Observable<Notification<PropertyResolver>> liveQueryForStatement(SqlStatement statement) {
        return liveQueriesCache.computeIfAbsent(statement, s -> statementExecutor
                .executeLiveQuery(s)
                .doOnNext(n -> log.trace("Received sequence number: {}", n.sequenceNumber()))
                .doFinally(() -> liveQueriesCache.remove(s))
                .share());
    }

    @Override
    public <K, S, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        TypeToken<T> elementType = HasMapping.objectType(query);
        ObjectExpression<T, R> aggregation = aggregator.apply(CollectionExpression.indirectArg(MoreTypeTokens.collection(elementType)));
        TypeToken<R> resultType = aggregation.reflect().objectType();
        return schemaGenerator.useTable(query.metaClass()).andThen(statementExecutor
                .executeQuery(statementProvider.forAggregation(query, aggregation, SqlFields.aggregationField))
                .map(pr -> {
                    Object obj = pr.getProperty(SqlFields.aggregationField, resultType);
                    //noinspection unchecked
                    return (obj instanceof PropertyResolver)
                            ? ((PropertyResolver)obj).toObject(resultType)
                            : (R)obj;
                })
                .firstElement());
    }

    @Override
    public <K, S> Single<Integer> update(UpdateInfo<K, S> update) {
        return schemaGenerator
                .useTable(update.metaClass())
                .andThen(statementExecutor.executeCommandReturnCount(statementProvider.forUpdate(update)));
    }

    @Override
    public <K, S> Single<Integer> delete(DeleteInfo<K, S> deleteInfo) {
        return schemaGenerator
                .useTable(deleteInfo.metaClass())
                .andThen(statementExecutor.executeCommandReturnCount(statementProvider.forDelete(deleteInfo)));
    }

    @Override
    public <K, S> Completable drop(MetaClassWithKey<K, S> metaClass) {
        return Completable.defer(() -> {
            SqlStatement statement = statementProvider.forDropTable(metaClass);
            return statementExecutor.executeCommand(statement);
        });
    }

    @Override
    public Completable dropAll() {
        return Completable.defer(() -> {
            SqlStatement statement = statementProvider.forDropSchema();
            return statementExecutor.executeCommand(statement)
                    .doOnComplete(schemaGenerator::clear);
        });
    }
}
