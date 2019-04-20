package com.slimgears.rxrepo.sql;

import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;

import java.util.function.Function;
import java.util.stream.Stream;

import static com.slimgears.rxrepo.sql.StatementUtils.concat;

public class DefaultSqlAssignmentGenerator implements SqlAssignmentGenerator {
    private final SqlExpressionGenerator sqlExpressionGenerator;

    public DefaultSqlAssignmentGenerator(SqlExpressionGenerator sqlExpressionGenerator) {
        this.sqlExpressionGenerator = sqlExpressionGenerator;
    }

    @Override
    public <T> Function<PropertyMeta<T, ?>, Stream<String>> toAssignment(T object, ReferenceResolver referenceResolver) {
        return prop -> {
            Object obj = prop.getValue(object);
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
