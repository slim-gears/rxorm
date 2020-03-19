package com.slimgears.rxrepo.query.provider;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.util.PropertyExpressions;
import com.slimgears.rxrepo.util.PropertyMetas;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public static <K, S, T> QueryInfo<K, S, T> includeMandatoryProperties(QueryInfo<K, S, T> queryInfo) {
        return queryInfo.properties().isEmpty()
                ? queryInfo
                : queryInfo.toBuilder()
                .apply(includeMandatoryProperties(queryInfo.properties(), queryInfo.objectType()))
                .build();
    }

    private static <K, S, T> Consumer<QueryInfo.Builder<K, S, T>> includeMandatoryProperties(ImmutableSet<PropertyExpression<T, ?, ?>> properties, TypeToken<T> typeToken) {
        return builder -> builder.propertiesAddAll(PropertyExpressions.includeMandatoryProperties(typeToken, properties));
    }
}
