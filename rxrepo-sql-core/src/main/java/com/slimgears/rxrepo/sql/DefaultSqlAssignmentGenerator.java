package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.stream.Lazy;

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.slimgears.rxrepo.sql.StatementUtils.concat;

public class DefaultSqlAssignmentGenerator implements SqlAssignmentGenerator {
    private final SqlExpressionGenerator sqlExpressionGenerator;

    public DefaultSqlAssignmentGenerator(SqlExpressionGenerator sqlExpressionGenerator) {
        this.sqlExpressionGenerator = sqlExpressionGenerator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, T> Function<String, Stream<String>> toAssignment(
            MetaClassWithKey<K, T> metaClass,
            PropertyResolver propertyResolver,
            ReferenceResolver referenceResolver) {
        Lazy<T> object = Lazy.of(() -> propertyResolver.toObject(metaClass));
        return prop -> {
            Object val = Optional.ofNullable(metaClass.getProperty(prop))
                .map(p -> p.getValue(object.get()))
                .orElseGet(() -> propertyResolver.getProperty(prop, Object.class));

            if (val == null) {
                return Stream.empty();
            }

            //noinspection unchecked
            String valStr = (val instanceof HasMetaClassWithKey)
                    ? sqlExpressionGenerator.fromStatement(referenceResolver.toReferenceValue((HasMetaClassWithKey)val))
                    : sqlExpressionGenerator.fromConstant(val);
            String assignment = concat(toFullPropertyName(metaClass, prop), "=", valStr);
            return Stream.of(assignment);
        };
    }

    private <S> String toFullPropertyName(MetaClass<S> metaClass, String propertyName) {
        return Optional
                .ofNullable(metaClass.getProperty(propertyName))
                .map(sqlExpressionGenerator::fromProperty)
                .orElseGet(() -> "`" + propertyName + "`");
    }
}
