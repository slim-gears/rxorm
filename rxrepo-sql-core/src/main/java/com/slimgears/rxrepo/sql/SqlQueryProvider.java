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
import com.slimgears.rxrepo.query.decorator.CacheQueryProviderDecorator;
import com.slimgears.rxrepo.query.provider.*;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.rxrepo.util.PropertyResolvers;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeTokens;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;
import io.reactivex.*;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.slimgears.util.generic.LazyString.lazy;

public class SqlQueryProvider implements QueryProvider {
    private final static Logger log = LoggerFactory.getLogger(SqlQueryProvider.class);
    private final static String aggregationField = "__aggregation";
    protected final SqlStatementProvider statementProvider;
    private final SqlStatementExecutor statementExecutor;
    protected final SchemaProvider schemaProvider;
    private final ReferenceResolver referenceResolver;

    protected SqlQueryProvider(SqlStatementProvider statementProvider,
                     SqlStatementExecutor statementExecutor,
                     SchemaProvider schemaProvider,
                     ReferenceResolver referenceResolver) {
        this.statementProvider = statementProvider;
        this.statementExecutor = statementExecutor;
        this.schemaProvider = schemaProvider;
        this.referenceResolver = referenceResolver;
    }

    public static QueryProvider create(SqlServiceFactory serviceFactory) {
        return new SqlQueryProvider(
                serviceFactory.statementProvider(),
                serviceFactory.statementExecutor(),
                serviceFactory.schemaProvider(),
                serviceFactory.referenceResolver());
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return Optional
                .of(entities)
                .filter(e -> !Iterables.isEmpty(e))
                .map(meta -> schemaProvider.createOrUpdate(metaClass)
                        .doOnSubscribe(d -> log.debug("Beginning creating class {}", lazy(metaClass::simpleName)))
                        .doOnComplete(() -> log.debug("Finished creating class {}", lazy(metaClass::simpleName)))
                        .andThen(Observable.fromIterable(entities)
                                .flatMapSingle(e -> insert(metaClass, e))
                                .ignoreElements()))
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

        return schemaProvider.createOrUpdate(metaClass)
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
        return schemaProvider.createOrUpdate(metaClass)
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
    public <K, S, T> Observable<T> query(QueryInfo<K, S, T> query) {
        TypeToken<? extends T> objectType = HasMapping.objectType(query);
        return schemaProvider
                .createOrUpdate(query.metaClass())
                .andThen(statementExecutor
                        .executeQuery(statementProvider.forQuery(query))
                        .compose(toObjects(objectType, query.mapping(), query.properties())));
    }

    @SuppressWarnings("unchecked")
    private <T> ObservableTransformer<PropertyResolver, T> toObjects(TypeToken<? extends T> objectType,
                                                                     ObjectExpression<?, T> mapping,
                                                                     ImmutableSet<PropertyExpression<T, ?, ?>> properties) {
        Function<PropertyResolver, Maybe<T>> mapper = Optional
                .ofNullable(mapping)
                .flatMap(Optionals.ofType(PropertyExpression.class))
                .map(PropertyExpression::path)
                .<Function<PropertyResolver, Maybe<T>>>map(path -> pr -> Optional
                        .ofNullable(pr.getProperty(path, TypeTokens.asClass(objectType)))
                        .map(obj -> obj instanceof PropertyResolver
                                ? PropertyResolvers.withProperties(properties, () -> (PropertyResolver) obj).toObject(objectType)
                                : (T)obj)
                        .map(Maybe::just)
                        .orElseGet(Maybe::empty))
                .orElse(pr -> Maybe.fromCallable(() -> pr.toObject(objectType)));
        return src -> src.flatMapMaybe(mapper);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        TypeToken<? extends T> objectType = HasMapping.objectType(query);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Scheduler scheduler = Schedulers.from(executorService);
        return schemaProvider.createOrUpdate(query.metaClass()).andThen(statementExecutor
                .executeLiveQuery(statementProvider.forQuery(query.toBuilder().properties(ImmutableSet.of()).build()))
                .observeOn(scheduler)
                .doFinally(executorService::shutdown)
                .map(notification -> notification.map(pr -> PropertyResolvers.withProperties(query.properties(), () -> pr.toObject(objectType)))));
    }

    @Override
    public <K, S, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        TypeToken<T> elementType = HasMapping.objectType(query);
        ObjectExpression<T, R> aggregation = aggregator.apply(CollectionExpression.indirectArg(MoreTypeTokens.collection(elementType)));
        TypeToken<R> resultType = aggregation.reflect().objectType();
        return schemaProvider.createOrUpdate(query.metaClass()).andThen(statementExecutor
                .executeQuery(statementProvider.forAggregation(query, aggregation, aggregationField))
                .map(pr -> {
                    Object obj = pr.getProperty(aggregationField, TypeTokens.asClass(resultType));
                    //noinspection unchecked
                    return (obj instanceof PropertyResolver)
                            ? ((PropertyResolver)obj).toObject(resultType)
                            : (R)obj;
                })
                .firstElement());
    }

    @Override
    public <K, S> Single<Integer> update(UpdateInfo<K, S> update) {
        return schemaProvider
                .createOrUpdate(update.metaClass())
                .andThen(statementExecutor.executeCommandReturnCount(statementProvider.forUpdate(update)));
    }

    @Override
    public <K, S> Single<Integer> delete(DeleteInfo<K, S> deleteInfo) {
        return schemaProvider
                .createOrUpdate(deleteInfo.metaClass())
                .andThen(statementExecutor.executeCommandReturnCount(statementProvider.forDelete(deleteInfo)));
    }

    @Override
    public <K, S> Completable drop(MetaClassWithKey<K, S> metaClass) {
        return Completable.defer(() -> {
            SqlStatement statement = statementProvider.forDrop(metaClass);
            return statementExecutor.executeCommand(statement);
        });
    }

    @Override
    public Completable dropAll() {
        return Completable.defer(() -> {
            SqlStatement statement = statementProvider.forDrop();
            return statementExecutor.executeCommand(statement)
                    .doOnComplete(schemaProvider::clear);
        });
    }

    private static <S extends HasMetaClass<S>> boolean isEmptyObject(S object) {
        AtomicReference<PropertyMeta<S, ?>> nonNullProperty = new AtomicReference<>();
        if (Streams.fromIterable(object.metaClass().properties())
                .filter(p -> p.getValue(object) != null)
                .peek(nonNullProperty::set)
                .limit(2)
                .count() > 1) {
            return false;
        }

        return Optional.ofNullable(nonNullProperty.get())
                .map(PropertyMetas::isKey)
                .orElse(true);
    }
}
