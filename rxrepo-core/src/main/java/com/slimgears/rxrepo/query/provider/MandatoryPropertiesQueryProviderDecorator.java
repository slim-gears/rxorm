package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.stream.Streams;
import io.reactivex.Observable;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MandatoryPropertiesQueryProviderDecorator implements QueryProvider.Decorator {
    @Override
    public QueryProvider apply(QueryProvider queryProvider) {
        return decorate(queryProvider);
    }

    public static QueryProvider decorate(QueryProvider queryProvider) {
        return new Decorator(queryProvider);
    }

    private static class Decorator extends AbstractQueryProviderDecorator {
        private Decorator(QueryProvider underlyingProvider) {
            super(underlyingProvider);
        }

        @Override
        public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> query(QueryInfo<K, S, T> query) {
            return query.properties().isEmpty()
                    ? super.query(query)
                    : super.query(query.toBuilder()
                            .apply(includeProperties(query.properties(), query.objectType().asClass()))
                            .build());
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, S extends HasMetaClassWithKey<K, S>, T> Consumer<QueryInfo.Builder<K, S, T>> includeProperties(Collection<PropertyExpression<T, ?, ?>> properties, Class<? extends T> cls) {
        return builder -> {
            Stream<PropertyExpression<T, ?, ?>> includedProperties = properties.stream()
                    .flatMap(MandatoryPropertiesQueryProviderDecorator::parentProperties);

            if (HasMetaClass.class.isAssignableFrom(cls)) {
                MetaClass<T> metaClass = MetaClasses.forClass((Class)cls);
                includedProperties = Stream.concat(includedProperties, mandatoryProperties(metaClass));
            }

            Collection<? extends PropertyExpression<T, ?, ?>> requiredProperties = includedProperties
                    .collect(Collectors.toMap(PropertyExpression::property, p -> p, (a, b) -> a, LinkedHashMap::new))
                    .values();

            builder.propertiesAddAll(requiredProperties);
        };
    }

    private static <S> Stream<PropertyExpression<S, ?, ?>> mandatoryProperties(MetaClass<S> metaClass) {
        return mandatoryProperties(ObjectExpression.arg(metaClass.objectClass()), metaClass);
    }

    @SuppressWarnings("unchecked")
    private static <S, T> Stream<PropertyExpression<S, ?, ?>> mandatoryProperties(ObjectExpression<S, T> target, MetaClass<T> metaClass) {
        return Streams.fromIterable(metaClass.properties())
                .filter(p -> !p.hasAnnotation(Nullable.class))
                .flatMap(p -> {
                    PropertyExpression<S, T, ?> propertyExpression = PropertyExpression.ofObject(target, p);
                    Stream<PropertyExpression<S, ?, ?>> stream = (Stream<PropertyExpression<S, ?, ?>>) Optional.of(p.type().asClass())
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
