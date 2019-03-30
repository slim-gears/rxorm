package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.HasEntityMeta;
import com.slimgears.rxrepo.query.provider.HasLimit;
import com.slimgears.rxrepo.query.provider.HasMapping;
import com.slimgears.rxrepo.query.provider.HasPagination;
import com.slimgears.rxrepo.query.provider.HasPredicate;
import com.slimgears.rxrepo.query.provider.HasProperties;
import com.slimgears.rxrepo.query.provider.HasSortingInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.SortingInfo;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.slimgears.rxrepo.sql.SqlStatement.of;

public class DefaultSqlStatementProvider implements SqlStatementProvider {
    private final SqlExpressionGenerator sqlExpressionGenerator;
    private final SchemaProvider schemaProvider;

    public DefaultSqlStatementProvider(SqlExpressionGenerator sqlExpressionGenerator, SchemaProvider schemaProvider) {
        this.sqlExpressionGenerator = sqlExpressionGenerator;
        this.schemaProvider = schemaProvider;
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> SqlStatement forQuery(QueryInfo<K, S, T> queryInfo) {
        return statement(() -> of(
                selectClause(queryInfo),
                fromClause(queryInfo),
                whereClause(queryInfo),
                orderClause(queryInfo),
                limitClause(queryInfo),
                skipClause(queryInfo)));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T, R> SqlStatement forAggregation(QueryInfo<K, S, T> queryInfo, ObjectExpression<T, R> aggregation, String projectedName) {
        return statement(() -> of(
                selectClause(queryInfo, aggregation, projectedName),
                fromClause(queryInfo),
                whereClause(queryInfo)));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> SqlStatement forUpdate(UpdateInfo<K, S> updateInfo) {
        return statement(() -> of(
                "update",
                schemaProvider.tableName(updateInfo.metaClass()),
                "set",
                updateInfo.propertyUpdates()
                        .stream()
                        .map(pu -> concat(sqlExpressionGenerator.toSqlExpression(pu.property()), "=", sqlExpressionGenerator.toSqlExpression(pu.updater())))
                        .collect(Collectors.joining(", ")),
                "return after",
                whereClause(updateInfo),
                limitClause(updateInfo)));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> SqlStatement forDelete(DeleteInfo<K, S> deleteInfo) {
        return statement(() -> of(
                "delete",
                fromClause(deleteInfo),
                whereClause(deleteInfo),
                limitClause(deleteInfo)));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> SqlStatement forInsertOrUpdate(S entity, ReferenceResolver resolver) {
        MetaClassWithKey<K, S> metaClass = entity.metaClass();
        PropertyMeta<S, K> keyProperty = metaClass.keyProperty();

        return statement(() -> of(
                        "update",
                        schemaProvider.tableName(entity.metaClass()),
                        "set",
                        Streams
                                .fromIterable(metaClass.properties())
                                .flatMap(toAssignment(entity, resolver))
                                .collect(Collectors.joining(", ")),
                        "upsert",
                        "return after",
                        "where",
                        toConditionClause(PropertyExpression.ofObject(keyProperty).eq(keyProperty.getValue(entity)))
                ));
    }

    private String concat(String... clauses) {
        return Arrays
                .stream(clauses).filter(c -> !c.isEmpty())
                .collect(Collectors.joining(" "));
    }

    private SqlStatement statement(Supplier<SqlStatement> statementSupplier) {
        List<Object> params = new ArrayList<>();
        SqlStatement statement = sqlExpressionGenerator.withParams(params, statementSupplier::get);
        return statement.withArgs(params.toArray());
    }

    private <K, S extends HasMetaClassWithKey<K, S>, T, Q extends HasMapping<S, T> & HasEntityMeta<K, S> & HasProperties<T>> String selectClause(Q queryInfo) {
        //noinspection unchecked
        ObjectExpression<S, T> expression = Optional
                .ofNullable(queryInfo.mapping())
                .orElse(ObjectExpression.<S>arg((TypeToken)queryInfo.metaClass().objectClass()));

        return Optional.of(toMappingClause(expression, queryInfo.properties()))
                .filter(exp -> !exp.isEmpty())
                .map(exp -> "select " + exp)
                .orElse("select");
    }

    private <K, S extends HasMetaClassWithKey<K, S>, T, R, Q extends HasMapping<S, T> & HasEntityMeta<K, S> & HasProperties<T>> String selectClause(Q statement, ObjectExpression<T, R> aggregation, String projectedName) {
        return concat(
                "select",
                Optional.ofNullable(statement.mapping())
                        .map(exp -> sqlExpressionGenerator.toSqlExpression(aggregation, exp))
                        .orElseGet(() -> sqlExpressionGenerator.toSqlExpression(aggregation)),
                "as",
                projectedName);
    }

    private <S, T> String toMappingClause(ObjectExpression<S, T> expression, Collection<PropertyExpression<T, ?, ?>> properies) {
        return Optionals.or(
                () -> Optional.ofNullable(properies)
                        .filter(p -> !p.isEmpty())
                        .map(p -> p.stream()
                                .map(prop -> sqlExpressionGenerator.toSqlExpression(prop, expression))
                                .collect(Collectors.joining(", "))),
                () -> Optional
                        .of(sqlExpressionGenerator.toSqlExpression(expression))
                        .filter(e -> !e.isEmpty()))
                .orElse("");
    }

    private <K, S extends HasMetaClassWithKey<K, S>, Q extends HasEntityMeta<K, S>> String fromClause(Q statement) {
        return "from " + schemaProvider.tableName(statement.metaClass());
    }

    private <S, Q extends HasPredicate<S>> String whereClause(Q statement) {
        return Optional
                .ofNullable(statement.predicate())
                .map(this::toConditionClause)
                .map(cond -> "where " + cond)
                .orElse("");
    }

    private <Q extends HasLimit> String limitClause(Q statement) {
        return Optional.ofNullable(statement.limit())
                .map(count -> "limit " + count)
                .orElse("");
    }

    private <Q extends HasPagination> String skipClause(Q statement) {
        return Optional.ofNullable(statement.skip())
                .map(count -> "skip " + count)
                .orElse("");
    }

    private <T, Q extends HasSortingInfo<T>> String orderClause(Q statement) {
        if (statement.sorting().isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        SortingInfo<T, ?, ?> first = statement.sorting().get(0);
        builder.append("order by ");
        builder.append(toOrder(first));
        statement.sorting().stream().skip(1)
                .forEach(si -> {
                    builder.append(" then by ");
                    builder.append(toOrder(si));
                });
        return builder.toString();
    }

    private String toOrder(SortingInfo<?, ?, ?> sortingInfo) {
        return sqlExpressionGenerator.toSqlExpression(sortingInfo.property()) + (sortingInfo.ascending() ? " asc" : " desc");
    }

    private <S> String toConditionClause(ObjectExpression<S, Boolean> condition) {
        return sqlExpressionGenerator.toSqlExpression(condition);
    }

    private <T> Function<PropertyMeta<T, ?>, Stream<String>> toAssignment(T entity, ReferenceResolver referenceResolver) {
        return prop -> {
            Object obj = prop.getValue(entity);
            if (obj == null) {
                return Stream.empty();
            }
            //noinspection unchecked
            String val = (obj instanceof HasMetaClassWithKey)
                    ? sqlExpressionGenerator.fromStatement(referenceResolver.toReferenceValue((HasMetaClassWithKey)obj))
                    : sqlExpressionGenerator.fromConstant(obj);
            String assignment = concat(sqlExpressionGenerator.fromProperty(prop), "=", val);
            return Stream.of(assignment);
        };
    }
}
