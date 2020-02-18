package com.slimgears.rxrepo.query.provider;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSet;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.util.PropertyExpressions;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

public class QueryInfos {
    public static <K, S, T> ImmutableSet<PropertyExpression<S, ?, ?>> allReferencedProperties(QueryInfo<K, S, T> query) {
        return ImmutableSet.<PropertyExpression<S, ?, ?>>builder()
                .addAll(PropertyExpressions.unmapProperties(query.properties(), query.mapping()))
                .addAll(PropertyExpressions.allReferencedProperties(query.mapping()))
                .addAll(PropertyExpressions.allReferencedProperties(query.predicate()))
                .addAll(query.sorting().stream()
                        .map(SortingInfo::property)
                        .map(PropertyExpressions::allReferencedProperties)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList()))
                .build();
    }

    @SuppressWarnings("unchecked")
    public static <K, S, T> QueryInfo<K, S, S> unmapQuery(QueryInfo<K, S, T> query) {
        return Optional.ofNullable(query.mapping())
                .filter(m -> m.type().operationType() != Expression.OperationType.Argument)
                .map(m -> QueryInfo
                        .<K, S, S>builder()
                        .metaClass(query.metaClass())
                        .sorting(query.sorting())
                        .predicate(query.predicate())
                        .limit(query.limit())
                        .skip(query.skip())
                        .properties(PropertyExpressions.unmapProperties(query.properties(), query.mapping()))
                        .build())
                .orElseGet(() -> (QueryInfo<K, S, S>)query);
    }
}
