package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;

public interface SelectQueryBuilder<K, S extends HasMetaClassWithKey<K, S>>
    extends QueryBuilder<SelectQueryBuilder<K, S>, K, S> {
    <V> SelectQueryBuilder<K, S> orderBy(PropertyExpression<S, S, V> field, boolean ascending);

    default <V> SelectQueryBuilder<K, S> orderBy(PropertyExpression<S, S, V> field) {
        return orderBy(field, true);
    }

    SelectQuery<S> select();

    <T> SelectQuery<T> select(ObjectExpression<S, T> expression);

    LiveSelectQuery<S> liveSelect();

    <T> LiveSelectQuery<T> liveSelect(ObjectExpression<S, T> expression);
}
