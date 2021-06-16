package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.sql.DefaultSqlAssignmentGenerator;
import com.slimgears.rxrepo.sql.KeyEncoder;
import com.slimgears.rxrepo.sql.ReferenceResolver;
import com.slimgears.rxrepo.sql.SqlExpressionGenerator;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.generic.MoreStrings;

import java.util.function.Function;
import java.util.stream.Stream;

import static com.slimgears.rxrepo.sql.StatementUtils.concat;

class OrientDbAssignmentGenerator extends DefaultSqlAssignmentGenerator {
    private final SqlExpressionGenerator sqlExpressionGenerator;
    private final KeyEncoder keyEncoder;

    OrientDbAssignmentGenerator(SqlExpressionGenerator sqlExpressionGenerator, KeyEncoder keyEncoder) {
        super(sqlExpressionGenerator);
        this.sqlExpressionGenerator = sqlExpressionGenerator;
        this.keyEncoder = keyEncoder;
    }

    @Override
    public <K, T> Function<String, Stream<String>> toAssignment(
            MetaClassWithKey<K, T> metaClass,
            PropertyResolver propertyResolver,
            ReferenceResolver referenceResolver) {
        Function<String, Stream<String>> inherited = super.toAssignment(metaClass, propertyResolver, referenceResolver);
        return prop -> Stream.concat(
                inherited.apply(prop),
                enhanceAssignmentForAsStringIndex(metaClass, propertyResolver, prop));
    }

    private String sequenceNumberAssignment() {
        return MoreStrings.format("{} = sequence('{}').next()",
                OrientDbQueryProvider.sequenceNumField,
                OrientDbSchemaProvider.sequenceName);

    }

    private <K, T> Stream<String> enhanceAssignmentForAsStringIndex(MetaClassWithKey<K, T> metaClass, PropertyResolver propertyResolver, String propertyName) {
        PropertyMeta<T, ?> propertyMeta = metaClass.getProperty(propertyName);

        if (propertyMeta != null && PropertyMetas.isEmbedded(propertyMeta)) {
            Object val = propertyResolver.getProperty(propertyMeta);
            return val != null
                    ? Stream.of(concat(
                            (sqlExpressionGenerator.fromProperty(propertyMeta) + "`AsString`").replace("``", ""),
                            "=",
                            sqlExpressionGenerator.fromConstant(keyEncoder.encode(val))),
                            sequenceNumberAssignment())
                    : Stream.empty();
        }

        return Stream.empty();
    }
}
