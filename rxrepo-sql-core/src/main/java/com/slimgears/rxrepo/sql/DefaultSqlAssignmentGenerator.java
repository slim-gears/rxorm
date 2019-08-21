package com.slimgears.rxrepo.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.internal.MoreTypeTokens;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
        return prop -> {
            Object obj = propertyResolver.getProperty(prop, Object.class);
            if (obj == null) {
                return Stream.empty();
            }

            Object convertedObj = Optional.ofNullable(metaClass.getProperty(prop))
                    .map(PropertyMeta::type)
                    .map(t -> convertObject(obj, t))
                    .orElse(obj);

            //noinspection unchecked
            String val = (convertedObj instanceof HasMetaClassWithKey)
                    ? sqlExpressionGenerator.fromStatement(referenceResolver.toReferenceValue((HasMetaClassWithKey)convertedObj))
                    : sqlExpressionGenerator.fromConstant(convertedObj);
            String assignment = concat(toFullPropertyName(metaClass, prop), "=", val);
            return Stream.of(assignment);
        };
    }

    @SuppressWarnings("unchecked")
    private Object convertObject(Object obj, TypeToken type) {
        if (obj instanceof PropertyResolver) {
            MetaClass valMeta = MetaClasses.forToken(type);
            return ((PropertyResolver)obj).toObject(valMeta);
        } else if (obj instanceof List) {
            TypeToken elementType = MoreTypeTokens.elementType(type);
            return ((List<?>) obj).stream()
                    .map(o -> convertObject(o, elementType))
                    .collect(ImmutableList.toImmutableList());
        } else if (obj instanceof Set) {
            TypeToken elementType = MoreTypeTokens.elementType(type);
            return ((Set<?>) obj).stream()
                    .map(o -> convertObject(o, elementType))
                    .collect(ImmutableSet.toImmutableSet());
        } else if (obj instanceof Map) {
            TypeToken keyType = MoreTypeTokens.keyType(type);
            TypeToken valType = MoreTypeTokens.valueType(type);
            return ((Map<?, ?>) obj).entrySet()
                    .stream()
                    .collect(ImmutableMap.toImmutableMap(
                            e -> convertObject(e.getKey(), keyType),
                            e -> convertObject(e.getValue(), valType)));
        } else {
            return obj;
        }
    }

    private <S> String toFullPropertyName(MetaClass<S> metaClass, String propertyName) {
        return Optional
                .ofNullable(metaClass.getProperty(propertyName))
                .map(sqlExpressionGenerator::fromProperty)
                .orElseGet(() -> "`" + propertyName + "`");
    }
}
