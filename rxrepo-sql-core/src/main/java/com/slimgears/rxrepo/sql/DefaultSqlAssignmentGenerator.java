package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;

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
    public <K, T extends HasMetaClassWithKey<K, T>> Function<String, Stream<String>> toAssignment(
            MetaClassWithKey<K, T> metaClass,
            PropertyResolver propertyResolver,
            ReferenceResolver referenceResolver) {
        return prop -> {
            Object obj = propertyResolver.getProperty(prop, Object.class);
            if (obj == null) {
                return Stream.empty();
            }

            if (obj instanceof PropertyResolver) {
                MetaClass valMeta = MetaClasses.forToken(((PropertyMeta)metaClass.getProperty(prop)).type());
                obj = ((PropertyResolver)obj).toObject(valMeta);
            }

            //noinspection unchecked
            String val = (obj instanceof HasMetaClassWithKey)
                    ? sqlExpressionGenerator.fromStatement(referenceResolver.toReferenceValue((HasMetaClassWithKey)obj))
                    : sqlExpressionGenerator.fromConstant(obj);
            String assignment = concat(toFullPropertyName(metaClass, prop), "=", val);
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
