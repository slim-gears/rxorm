package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.sql.DefaultSqlAssignmentGenerator;
import com.slimgears.rxrepo.sql.PropertyMetas;
import com.slimgears.rxrepo.sql.ReferenceResolver;
import com.slimgears.rxrepo.sql.SqlExpressionGenerator;
import com.slimgears.util.autovalue.annotations.PropertyMeta;

import java.util.function.Function;
import java.util.stream.Stream;

import static com.slimgears.rxrepo.sql.StatementUtils.concat;

class OrientDbAssignmentGenerator extends DefaultSqlAssignmentGenerator {
    private final SqlExpressionGenerator sqlExpressionGenerator;

    OrientDbAssignmentGenerator(SqlExpressionGenerator sqlExpressionGenerator) {
        super(sqlExpressionGenerator);
        this.sqlExpressionGenerator = sqlExpressionGenerator;
    }

    @Override
    public <T> Function<PropertyMeta<T, ?>, Stream<String>> toAssignment(T object, ReferenceResolver referenceResolver) {
        Function<PropertyMeta<T, ?>, Stream<String>> inherited = super.toAssignment(object, referenceResolver);
        return prop -> Stream.concat(
                inherited.apply(prop),
                enhanceAssignmentForAsStringIndex(object, prop));
    }

    private <T> Stream<String> enhanceAssignmentForAsStringIndex(T object, PropertyMeta<T, ?> propertyMeta) {
        if (PropertyMetas.isIndexableByString(propertyMeta)) {
            Object val = propertyMeta.getValue(object);
            return val != null
                    ? Stream.of(concat(sqlExpressionGenerator.fromProperty(propertyMeta) + "AsString", "=", sqlExpressionGenerator.fromConstant(val.toString())))
                    : Stream.empty();
        }

        return Stream.empty();
    }
}
