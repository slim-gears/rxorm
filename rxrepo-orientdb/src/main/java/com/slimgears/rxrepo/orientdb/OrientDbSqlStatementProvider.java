package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.sql.*;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.generic.MoreStrings;
import com.slimgears.util.stream.Streams;

import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.slimgears.rxrepo.sql.SqlStatement.of;

public class OrientDbSqlStatementProvider extends DefaultSqlStatementProvider {
    public OrientDbSqlStatementProvider(SqlExpressionGenerator sqlExpressionGenerator, SqlTypeMapper typeMapper, Supplier<String> dbNameSupplier) {
        super(sqlExpressionGenerator, typeMapper, dbNameSupplier);
    }

    @Override
    protected <K, S> SqlStatement forInsertOrUpdate(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver, ReferenceResolver resolver, boolean forced) {
        PropertyMeta<S, K> keyProperty = metaClass.keyProperty();

        return statement(() -> {
            Map<String, String> assignments = toAssignments(metaClass, propertyResolver, resolver)
                    .collect(Collectors.toMap(Assignment::name, Assignment::value));
            return of(
                    "update",
                    tableName(metaClass),
                    "set",
                    Streams
                            .fromIterable(propertyResolver.propertyNames())
                            .map(name -> MoreStrings.format("`{}` = {}", name, assignments.get(name)))
                            .collect(Collectors.joining(", ")),
                    forced ? "upsert" : "",
                    "return after",
                    "where",
                    toConditionClause(PropertyExpression.ofObject(keyProperty).eq(propertyResolver.getProperty(keyProperty)))
            );
        });
    }

    @Override
    protected <S, T> Stream<String> toProjectionFields(ObjectExpression<S, T> expression, Collection<PropertyExpression<T, ?, ?>> properties) {
        return Stream.concat(
                super.toProjectionFields(expression, properties),
                Stream.of("`" + DefaultSqlQueryProvider.sequenceNumField + "`"));
    }
}
