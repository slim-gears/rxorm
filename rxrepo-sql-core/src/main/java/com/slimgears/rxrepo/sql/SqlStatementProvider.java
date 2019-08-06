package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

public interface SqlStatementProvider {
    <K, S extends HasMetaClassWithKey<K, S>, T> SqlStatement forQuery(QueryInfo<K, S, T> queryInfo);
    <K, S extends HasMetaClassWithKey<K, S>, T, R> SqlStatement forAggregation(QueryInfo<K, S, T> queryInfo,
                                                                               ObjectExpression<T, R> aggregation,
                                                                               String projectedName);
    <K, S extends HasMetaClassWithKey<K, S>> SqlStatement forUpdate(UpdateInfo<K, S> updateInfo);
    <K, S extends HasMetaClassWithKey<K, S>> SqlStatement forDelete(DeleteInfo<K, S> deleteInfo);
    <K, S extends HasMetaClassWithKey<K, S>> SqlStatement forInsert(MetaClassWithKey<K, S> metaClass,
                                                                    PropertyResolver propertyResolver,
                                                                    ReferenceResolver referenceResolver);
    <K, S extends HasMetaClassWithKey<K, S>> SqlStatement forInsertOrUpdate(MetaClassWithKey<K, S> metaClass,
                                                                            PropertyResolver propertyResolver,
                                                                            ReferenceResolver referenceResolver);

    <K, S extends HasMetaClassWithKey<K, S>> SqlStatement forDrop(MetaClassWithKey<K, S> metaClass);
    SqlStatement forDrop();

    default <K, S extends HasMetaClassWithKey<K, S>> SqlStatement forInsertOrUpdate(S entity, ReferenceResolver referenceResolver) {
        return forInsertOrUpdate(entity.metaClass(), PropertyResolver.fromObject(entity), referenceResolver);
    }

    default <K, S extends HasMetaClassWithKey<K, S>> SqlStatement forInsert(S entity, ReferenceResolver referenceResolver) {
        return forInsert(entity.metaClass(), PropertyResolver.fromObject(entity), referenceResolver);
    }
}
