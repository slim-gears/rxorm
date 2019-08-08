package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.reflect.TypeTokens;
import com.slimgears.util.stream.Streams;
import io.reactivex.Observable;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MandatoryPropertiesQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final static Map<Class<?>, List<PropertyExpression<?, ?, ?>>> mandatoryPropertiesCache = new ConcurrentHashMap<>();

    private MandatoryPropertiesQueryProviderDecorator(QueryProvider underlyingProvider) {
        super(underlyingProvider);
    }

    public static QueryProvider.Decorator create() {
        return MandatoryPropertiesQueryProviderDecorator::new;
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> query(QueryInfo<K, S, T> query) {
        return query.properties().isEmpty()
                ? super.query(query)
                : super.query(query.toBuilder()
                        .apply(includeProperties(query.properties(), TypeTokens.asClass(query.objectType())))
                        .build());
    }

    private static <K, S extends HasMetaClassWithKey<K, S>, T> Consumer<QueryInfo.Builder<K, S, T>> includeProperties(Collection<PropertyExpression<T, ?, ?>> properties, Class<T> cls) {
        return builder -> {
            Stream<PropertyExpression<T, ?, ?>> includedProperties = properties.stream()
                    .flatMap(MandatoryPropertiesQueryProviderDecorator::parentProperties);

            if (HasMetaClass.class.isAssignableFrom(cls)) {
                includedProperties = Stream.concat(includedProperties, mandatoryProperties(cls));
            }

            Collection<? extends PropertyExpression<T, ?, ?>> requiredProperties = includedProperties
                    .collect(Collectors.toMap(PropertyExpression::property, p -> p, (a, b) -> a, LinkedHashMap::new))
                    .values();

            builder.propertiesAddAll(requiredProperties);
        };
    }

    @SuppressWarnings("unchecked")
    private static <S> Stream<PropertyExpression<S, ?, ?>> mandatoryProperties(Class<S> cls) {
        return mandatoryPropertiesCache.computeIfAbsent(
                cls,
                mc -> mandatoryPropertiesNotCached(MetaClasses.forClassUnchecked(cls))
                        .collect(Collectors.toList()))
                .stream()
                .map(p -> (PropertyExpression<S, ?, ?>)p);
    }

    private static <S> Stream<PropertyExpression<S, ?, ?>> mandatoryPropertiesNotCached(MetaClass<S> metaClass) {
        return mandatoryProperties(ObjectExpression.arg(metaClass.asType()), metaClass);
    }

    @SuppressWarnings("unchecked")
    private static <S, T> Stream<PropertyExpression<S, ?, ?>> mandatoryProperties(ObjectExpression<S, T> target, MetaClass<T> metaClass) {
        return Streams.fromIterable(metaClass.properties())
                .filter(p -> !p.hasAnnotation(Nullable.class))
                .flatMap(p -> {
                    PropertyExpression<S, T, ?> propertyExpression = PropertyExpression.ofObject(target, p);
                    Stream<PropertyExpression<S, ?, ?>> stream = (Stream<PropertyExpression<S, ?, ?>>) Optional.of(TypeTokens.asClass(p.type()))
                            .filter(HasMetaClass.class::isAssignableFrom)
                            .map(cls -> (Class) cls)
                            .map(MetaClasses::forClass)
                            .map(meta -> mandatoryProperties(propertyExpression, (MetaClass) meta))
                            .orElseGet(Stream::empty);
                    return Stream.concat(Stream.of(propertyExpression), stream);
                });
    }

    private static <S, T, V> Stream<PropertyExpression<S, ?, ?>> parentProperties(PropertyExpression<S, T, V> property) {
        return (property.target() instanceof PropertyExpression)
                ? Stream.concat(Stream.of(property), parentProperties((PropertyExpression<S, ?, ?>)property.target()))
                : Stream.of(property);
    }
}
