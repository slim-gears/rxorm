package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.filters.Filter;
import com.slimgears.rxrepo.util.Expressions;

import java.util.Optional;

class MappedSelectQueryBuilder<T, S> extends SelectQueryBuilder<T> {
    private final SelectQueryBuilder<S> underlying;
    private final ObjectExpression<S, T> mapper;

    static <K, T, S> SelectQueryBuilder<T> create(SelectQueryBuilder<S> underlying, ObjectExpression<S, T> mapper) {
        return new MappedSelectQueryBuilder<>(underlying, mapper);
    }

    private MappedSelectQueryBuilder(SelectQueryBuilder<S> underlying, ObjectExpression<S, T> mapper) {
        this.underlying = underlying;
        this.mapper = mapper;
    }

    @Override
    public SelectQueryBuilder<T> skip(long skip) {
        underlying.skip(skip);
        return this;
    }

    @Override
    public <V extends Comparable<V>> SelectQueryBuilder<T> orderBy(PropertyExpression<T, ?, V> field, boolean ascending) {
        underlying.orderBy(Expressions.compose(mapper, field), ascending);
        return this;
    }

    @Override
    public SelectQuery<T> select() {
        return underlying.select(mapper);
    }

    @Override
    public <T1> SelectQuery<T1> select(ObjectExpression<T, T1> expression, boolean distinct) {
        return underlying.select(Expressions.compose(mapper, expression), distinct);
    }

    @Override
    public LiveSelectQuery<T> liveSelect() {
        return underlying.liveSelect(mapper);
    }

    @Override
    public <T1> LiveSelectQuery<T1> liveSelect(ObjectExpression<T, T1> expression) {
        return underlying.liveSelect(Expressions.compose(mapper, expression));
    }

    @Override
    public SelectQueryBuilder<T> where(ObjectExpression<T, Boolean> predicate) {
        underlying.where(Expressions.compose(mapper, predicate));
        return this;
    }

    @Override
    public SelectQueryBuilder<T> limit(long limit) {
        underlying.limit(limit);
        return this;
    }

    @Override
    public SelectQueryBuilder<T> where(Filter<T> filter) {
        Optional.ofNullable(filter)
            .flatMap(f -> f.toExpression(mapper))
            .ifPresent(underlying::where);
        return this;
    }
}
