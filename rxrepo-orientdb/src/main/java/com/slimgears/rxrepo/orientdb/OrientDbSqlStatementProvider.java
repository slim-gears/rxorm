package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.sql.*;
import com.slimgears.rxrepo.util.PropertyExpressionValueProvider;
import com.slimgears.rxrepo.util.PropertyExpressions;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.generic.MoreStrings;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.slimgears.rxrepo.sql.SqlStatement.of;
import static com.slimgears.rxrepo.sql.StatementUtils.concat;

@SuppressWarnings("UnstableApiUsage")
public class OrientDbSqlStatementProvider extends DefaultSqlStatementProvider {
    private final KeyEncoder keyEncoder;

    public OrientDbSqlStatementProvider(SqlExpressionGenerator sqlExpressionGenerator,
                                        SqlTypeMapper typeMapper,
                                        KeyEncoder keyEncoder,
                                        Supplier<String> dbNameSupplier) {
        super(sqlExpressionGenerator, typeMapper, dbNameSupplier);
        this.keyEncoder = keyEncoder;
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
    protected <K, S> SqlStatement forInsertStatement(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver, ReferenceResolver resolver) {
        Collection<Assignment> assignments = toAssignments(metaClass, propertyResolver, resolver)
                .collect(Collectors.toList());

        return statement(() -> of(
                "insert",
                "into",
                fullTableName(metaClass),
                "set",
                assignments
                        .stream()
                        .map(a -> concat(fullFieldName(a.name()), "=", a.value()))
                        .collect(Collectors.joining(", ")),
                "return null"
        ));
    }

    @Override
    protected <S, T> Stream<String> toProjectionFields(MetaClassWithKey<?, S> metaClass, ObjectExpression<S, T> expression, Collection<PropertyExpression<T, ?, ?>> properties) {
        return Stream.concat(
                super.toProjectionFields(metaClass, expression, properties),
                Stream.of("`" + SqlFields.sequenceFieldName + "`"));
    }

    @Override
    protected String fullTableName(MetaClassWithKey<?, ?> metaClass) {
        return metaClass.simpleName();
    }

    @Override
    protected <K, T> Stream<PropertyExpression<T, ?, ?>> toAssignableProperties(MetaClassWithKey<K, T> metaClass) {
        return PropertyExpressions.ownPropertiesOf(metaClass.asType()).map(p -> p);
    }

    @Override
    protected <K, T> Stream<Assignment> toAssignments(MetaClassWithKey<K, T> metaClass, PropertyResolver propertyResolver, ReferenceResolver referenceResolver) {
        return Stream.concat(
                super.toAssignments(metaClass, propertyResolver, referenceResolver),
                Stream.of(Assignment.of("`" + SqlFields.sequenceFieldName + "`", MoreStrings.format("sequence('{}').next()", OrientDbSchemaGenerator.sequenceName))));
    }

    @Override
    protected <T> Stream<Assignment> toAssignment(PropertyExpressionValueProvider<T, ?, ?> vp, T object, ReferenceResolver referenceResolver) {
        if (PropertyMetas.isEmbedded(vp.property().property())) {
            String name = sqlExpressionGenerator.toSqlExpression(vp.property()) + "`AsString`";
            name = name.replaceAll("``", "");
//            return Stream.of(Assignment.of(name, sqlExpressionGenerator.fromConstant(keyEncoder.encode(vp.value(object)))));
            return Stream.concat(
                    Stream.of(Assignment.of(name, Optional.ofNullable(vp.value(object))
                            .map(keyEncoder::encode)
                            .map(sqlExpressionGenerator::fromConstant)
                            .orElseGet(() -> sqlExpressionGenerator.fromNull(vp.property().reflect().objectType())))),
                    super.toAssignment(vp, object, referenceResolver));
        } else {
            return super.toAssignment(vp, object, referenceResolver);
        }
    }

    @Override
    protected <S, T> String toMappingClause(MetaClassWithKey<?, S> metaClass, ObjectExpression<S, T> expression, Collection<PropertyExpression<T, ?, ?>> properties) {
        return Optionals.or(
                () -> Optional.ofNullable(properties)
                        .filter(p -> !p.isEmpty())
                        .map(this::eliminateRedundantProperties)
                        .map(p -> Stream.concat(
                                p.stream().map(prop -> sqlExpressionGenerator.toSqlExpression(prop, expression)),
                                Stream.of("`" + SqlFields.sequenceFieldName + "`"))
                                .collect(Collectors.joining(", "))),
                () -> Optional
                        .of(sqlExpressionGenerator.toSqlExpression(expression))
                        .filter(e -> !e.isEmpty()))
                .orElse("");
    }
}
