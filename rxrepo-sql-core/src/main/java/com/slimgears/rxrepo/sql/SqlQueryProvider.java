package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.CollectionExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.HasMapping;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.reflect.TypeToken;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.util.Objects;

public class SqlQueryProvider implements QueryProvider {
    private final static String aggregationField = "__aggregation";
    private final SqlStatementProvider statementProvider;
    private final SqlStatementExecutor statementExecutor;
    private final SchemaProvider schemaProvider;
    private final ReferenceResolver referenceResolver;

    public SqlQueryProvider(SqlStatementProvider statementProvider,
                            SqlStatementExecutor statementExecutor,
                            SchemaProvider schemaProvider,
                            ReferenceResolver referenceResolver) {
        this.statementProvider = statementProvider;
        this.statementExecutor = statementExecutor;
        this.schemaProvider = schemaProvider;
        this.referenceResolver = referenceResolver;
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<S> insertOrUpdate(S entity) {
        MetaClassWithKey<K, S> metaClass = entity.metaClass();
        //noinspection unchecked
        Completable references = Observable
                .fromIterable(metaClass.properties())
                .filter(PropertyMetas::isReference)
                .map(prop -> prop.getValue(entity))
                .filter(Objects::nonNull)
                .ofType(HasMetaClassWithKey.class)
                .flatMapSingle(this::insertOrUpdate)
                .ignoreElements();

        return references.andThen(schemaProvider.createOrUpdate(entity.metaClass()).andThen(statementExecutor
                .executeCommandReturnEntries(statementProvider.forInsertOrUpdate(entity, referenceResolver))
                .map(pr -> pr.toObject(entity.metaClass()))
                .singleOrError()));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> query(QueryInfo<K, S, T> query) {
        TypeToken<? extends T> objectType = HasMapping.objectType(query);
        return schemaProvider.createOrUpdate(query.metaClass()).andThen(statementExecutor
                .executeQuery(statementProvider.forQuery(query))
                .map(pr -> pr.toObject(objectType)));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        TypeToken<? extends T> objectType = HasMapping.objectType(query);
        return schemaProvider.createOrUpdate(query.metaClass()).andThen(statementExecutor
                .executeLiveQuery(statementProvider.forQuery(query))
                .map(notification -> notification.map(pr -> pr.toObject(objectType))));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T, R> Single<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R, ?> aggregator) {
        TypeToken<? extends T> elementType = HasMapping.objectType(query);
        ObjectExpression<T, R> aggregation = aggregator.apply(CollectionExpression.indirectArg(elementType));
        TypeToken<? extends R> resultType = aggregation.objectType();
        return schemaProvider.createOrUpdate(query.metaClass()).andThen(statementExecutor
                .executeQuery(statementProvider.forAggregation(query, aggregation, aggregationField))
                .map(pr -> {
                    Object obj = pr.getProperty(aggregationField, resultType.asClass());
                    //noinspection unchecked
                    return (obj instanceof PropertyResolver)
                            ? ((PropertyResolver)obj).toObject(resultType)
                            : (R)obj;
                })
                .singleOrError());
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Observable<S> update(UpdateInfo<K, S> update) {
        return schemaProvider.createOrUpdate(update.metaClass()).andThen(statementExecutor
                .executeCommandReturnEntries(statementProvider.forUpdate(update))
                .map(pr -> pr.toObject(update.metaClass())));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<Integer> delete(DeleteInfo<K, S> deleteInfo) {
        return schemaProvider.createOrUpdate(deleteInfo.metaClass()).andThen(statementExecutor
               .executeCommandReturnCount(statementProvider.forDelete(deleteInfo)));
    }
}
