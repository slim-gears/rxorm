package com.slimgears.rxrepo.sql;

import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.CollectionExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.expressions.internal.MoreTypeTokens;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.*;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.reflect.TypeTokens;
import com.slimgears.util.stream.Optionals;
import io.reactivex.*;
import io.reactivex.functions.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class SqlQueryProvider implements QueryProvider {
    private final static Logger log = LoggerFactory.getLogger(SqlQueryProvider.class);
    private final static String aggregationField = "__aggregation";
    private final SqlStatementProvider statementProvider;
    private final SqlStatementExecutor statementExecutor;
    private final SchemaProvider schemaProvider;
    private final ReferenceResolver referenceResolver;

    SqlQueryProvider(SqlStatementProvider statementProvider,
                     SqlStatementExecutor statementExecutor,
                     SchemaProvider schemaProvider,
                     ReferenceResolver referenceResolver) {
        this.statementProvider = statementProvider;
        this.statementExecutor = statementExecutor;
        this.schemaProvider = schemaProvider;
        this.referenceResolver = referenceResolver;
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Completable insert(Iterable<S> entities) {
        return Observable.fromIterable(entities)
                .flatMapSingle(this::insert)
                .ignoreElements();
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<S> insertOrUpdate(S entity) {
        return insertOrUpdate(entity.metaClass(), PropertyResolver.fromObject(entity));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        SqlStatement statement = statementProvider.forQuery(QueryInfo
                .<K, S, S>builder()
                .metaClass(metaClass)
                .predicate(PropertyExpression.ofObject(metaClass.keyProperty()).eq(key))
                .limit(1L)
                .build());

        return statementExecutor
                .executeQuery(statement)
                .firstElement()
                .flatMap((PropertyResolver pr) -> {
                    S oldObj = pr.toObject(metaClass);
                    return entityUpdater
                            .apply(Maybe.just(oldObj))
                            .filter(newObj -> !newObj.equals(oldObj))
                            .map(newObj -> pr.mergeWith(PropertyResolver.fromObject(newObj)))
                            .filter(newPr -> !pr.equals(newPr))
                            .flatMap(newPr -> insertOrUpdate(metaClass, newPr).toMaybe())
                            .switchIfEmpty(Maybe.just(oldObj));
                })
                .switchIfEmpty(Maybe.defer(() -> entityUpdater
                        .apply(Maybe.empty())
                        .flatMap(e -> insert(e).toMaybe())));
    }


    private <K, S extends HasMetaClassWithKey<K, S>> Single<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver) {
        SqlStatement statement = statementProvider.forInsertOrUpdate(metaClass, propertyResolver, referenceResolver);
        return insertOrUpdate(metaClass, statement);
    }

    private <K, S extends HasMetaClassWithKey<K, S>> Single<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, SqlStatement statement) {
        return schemaProvider.createOrUpdate(metaClass)
                .doOnSubscribe(d -> log.trace("Ensuring class {}", metaClass.simpleName()))
                .doOnError(e -> log.trace("Error when updating class: {}", metaClass.simpleName(), e))
                .doOnComplete(() -> log.trace("Class updated {}", metaClass.simpleName()))
                .andThen(statementExecutor
                        .executeCommandReturnEntries(statement)
                        .map(pr -> pr.toObject(metaClass))
                        .doOnSubscribe(d -> log.trace("Executing statement: {}", statement.statement()))
                        .doOnError(e -> log.trace("Failed to execute statement: {}", statement.statement(), e))
                        .doOnComplete(() -> log.trace("Execution complete: {}", statement.statement()))
                        .doOnNext(obj -> log.trace("Updated {}", obj))
                        .take(1)
                        .singleOrError());
    }

    private <K, S extends HasMetaClassWithKey<K, S>> Single<S> insert(S entity) {
        MetaClassWithKey<K, S> metaClass = entity.metaClass();
        SqlStatement statement = statementProvider.forInsert(entity, referenceResolver);
        return insertOrUpdate(metaClass, statement);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> query(QueryInfo<K, S, T> query) {
        TypeToken<? extends T> objectType = HasMapping.objectType(query);
        return schemaProvider
                .createOrUpdate(query.metaClass())
                .andThen(statementExecutor
                        .executeQuery(statementProvider.forQuery(query))
                        .compose(toObjects(objectType, query.mapping())));
    }

    @SuppressWarnings("unchecked")
    private <T> ObservableTransformer<PropertyResolver, T> toObjects(TypeToken<? extends T> objectType, ObjectExpression<?, T> mapping) {
        Function<PropertyResolver, Maybe<T>> mapper = Optional
                .ofNullable(mapping)
                .flatMap(Optionals.ofType(PropertyExpression.class))
                .map(e -> (PropertyExpression<?, ?, T>)e)
                .map(this::toPropertyPath)
                .<Function<PropertyResolver, Maybe<T>>>map(path -> pr -> Optional
                        .ofNullable(pr.getProperty(path, TypeTokens.asClass(objectType)))
                        .map(obj -> obj instanceof PropertyResolver
                                ? ((PropertyResolver) obj).toObject(objectType)
                                : (T)obj)
                        .map(Maybe::just)
                        .orElseGet(Maybe::empty))
                .orElse(pr -> Maybe.fromCallable(() -> pr.toObject(objectType)));
        return src -> src.flatMapMaybe(mapper);
    }

    private String toPropertyPath(PropertyExpression<?, ?, ?> propertyExpression) {
        return propertyExpression.target() instanceof PropertyExpression
                ? toPropertyPath((PropertyExpression<?, ?, ?>)propertyExpression.target()) + "." + propertyExpression.property().name()
                : propertyExpression.property().name();
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        TypeToken<? extends T> objectType = HasMapping.objectType(query);
        return schemaProvider.createOrUpdate(query.metaClass()).andThen(statementExecutor
                .executeLiveQuery(statementProvider.forQuery(query))
                .map(notification -> notification.map(pr -> pr.toObject(objectType))));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        TypeToken<T> elementType = HasMapping.objectType(query);
        ObjectExpression<T, R> aggregation = aggregator.apply(CollectionExpression.indirectArg(MoreTypeTokens.collection(elementType)));
        TypeToken<R> resultType = aggregation.objectType();
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
    public <K, S extends HasMetaClassWithKey<K, S>> Single<Integer> update(UpdateInfo<K, S> update) {
        return schemaProvider
                .createOrUpdate(update.metaClass())
                .andThen(statementExecutor.executeCommandReturnCount(statementProvider.forUpdate(update)));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<Integer> delete(DeleteInfo<K, S> deleteInfo) {
        return schemaProvider
                .createOrUpdate(deleteInfo.metaClass())
                .andThen(statementExecutor.executeCommandReturnCount(statementProvider.forDelete(deleteInfo)));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Completable drop(MetaClassWithKey<K, S> metaClass) {
        return Completable.defer(() -> {
            SqlStatement statement = statementProvider.forDrop(metaClass);
            return statementExecutor.executeCommand(statement);
        });
    }

    @Override
    public Completable dropAll() {
        return Completable.defer(() -> {
            SqlStatement statement = statementProvider.forDrop();
            return statementExecutor.executeCommand(statement);
        });
    }
}
