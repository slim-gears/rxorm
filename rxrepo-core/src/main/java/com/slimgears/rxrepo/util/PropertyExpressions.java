package com.slimgears.rxrepo.util;

import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("WeakerAccess")
public class PropertyExpressions {
    private final static Map<PropertyExpression<?, ?, ?>, Collection<PropertyExpression<?, ?, ?>>> relatedMandatoryPropertiesCache = new ConcurrentHashMap<>();
    private final static Map<TypeToken<?>, Collection<PropertyExpression<?, ?, ?>>> mandatoryPropertiesCache = new ConcurrentHashMap<>();
    private final static Map<PropertyExpression<?, ?, ?>, Collection<PropertyExpression<?, ?, ?>>> parentProperties = new ConcurrentHashMap<>();

    public static String toPath(PropertyExpression<?, ?, ?> propertyExpression) {
        return Optional.of(propertyExpression.target())
                .flatMap(Optionals.ofType(PropertyExpression.class))
                .map(PropertyExpression::path)
                .map(p -> p + "." + propertyExpression.property().name())
                .orElseGet(propertyExpression.property()::name);
    }

    public static <T> Stream<PropertyExpression<T, ?, ?>> propertiesOf(TypeToken<T> type) {
        return PropertyMetas.hasMetaClass(type)
                ? propertiesOf(ObjectExpression.arg(type))
                : Stream.empty();
    }

    public static <T> Stream<PropertyExpression<T, T, ?>> ownPropertiesOf(TypeToken<T> type) {
        return PropertyMetas.hasMetaClass(type)
                ? ownPropertiesOf(ObjectExpression.arg(type))
                : Stream.empty();
    }

    public static <S, T> Stream<PropertyExpression<S, T, ?>> ownPropertiesOf(ObjectExpression<S, T> target) {
        MetaClass<T> metaClass = MetaClasses.forTokenUnchecked(target.objectType());
        return Streams
                .fromIterable(metaClass.properties())
                .map(prop -> PropertyExpression.ofObject(target, prop));
    }

    public static <S, T> Stream<PropertyExpression<S, ?, ?>> propertiesOf(ObjectExpression<S, T> target) {
        List<PropertyExpression<S, T, ?>> ownProps = ownPropertiesOf(target)
                .collect(Collectors.toList());

        return Stream.concat(
                ownProps.stream(),
                ownProps.stream()
                        .filter(p -> PropertyMetas.hasMetaClass(p.objectType()))
                        .flatMap(PropertyExpressions::propertiesOf));
    }

    public static <T> PropertyExpression<T, ?, ?> fromPath(TypeToken<T> origin, String path) {
        return createExpressionFromPath(ObjectExpression.arg(origin), path);
    }

    public static <T> PropertyExpression<T, ?, ?> fromPath(Class<T> origin, String path) {
        return fromPath(TypeToken.of(origin), path);
    }

    @SuppressWarnings("unchecked")
    public static <T, V> Function<T, V> toGetter(PropertyExpression<T, ?, V> propertyExpression) {
        if (propertyExpression.target() instanceof PropertyExpression) {
            Function<T, ?> getter = toGetter((PropertyExpression<T, ?, ?>)propertyExpression.target());
            return getter.andThen(val -> Optional
                    .ofNullable(val)
                    .map(v -> ((PropertyMeta<Object, V>)propertyExpression.property()).getValue(v))
                    .orElse(null));
        }
        return ((PropertyMeta<T, V>)propertyExpression.property())::getValue;
    }

    private static <S, T> PropertyExpression<S, ?, ?> createExpressionFromPath(ObjectExpression<S, T> target, String path) {
        String head = head(path);
        MetaClass<T> meta = Objects.requireNonNull(MetaClasses.forTokenUnchecked(target.objectType()));
        PropertyMeta<T, ?> prop = meta.getProperty(head);
        if (head.length() == path.length()) {
            return PropertyExpression.ofObject(target, prop);
        }
        return createExpressionFromPath(PropertyExpression.ofObject(target, prop), path.substring(head.length() + 1));
    }

    private static String head(String path) {
        int pos = path.indexOf('.');
        return pos >= 0 ? path.substring(0, pos) : path;
    }

    public static boolean propertyEquals(PropertyExpression<?, ?, ?> property, Object other) {
        return Optional.ofNullable(other)
                .filter(p -> property.hashCode() == p.hashCode())
                .flatMap(Optionals.ofType(PropertyExpression.class))
                .map(p -> Objects.equals(property.target(), p.target()) &&
                          Objects.equals(property.property(), p.property()))
                .orElse(false);
    }

    @SuppressWarnings("unchecked")
    public static <S> Stream<PropertyExpression<S, ?, ?>> mandatoryProperties(PropertyExpression<S, ?, ?> exp) {
        return relatedMandatoryPropertiesCache.computeIfAbsent(
                exp,
                PropertyExpressions::mandatoryPropertiesNotCached)
                .stream()
                .map(p -> (PropertyExpression<S, ?, ?>)p);
    }

    @SuppressWarnings("unchecked")
    public static <S> Stream<PropertyExpression<S, ?, ?>> mandatoryProperties(TypeToken<S> typeToken) {
        return mandatoryPropertiesCache.computeIfAbsent(
                typeToken,
                PropertyExpressions::mandatoryPropertiesNotCached)
                .stream()
                .map(p -> (PropertyExpression<S, ?, ?>)p);
    }

    private static <S> Collection<PropertyExpression<?, ?, ?>> mandatoryPropertiesNotCached(PropertyExpression<S, ?, ?> exp) {
        return Stream.concat(Stream.of(exp), parentProperties(exp)
                .filter(p -> PropertyMetas.hasMetaClass(p.property()))
                .flatMap(p -> mandatoryProperties(p, MetaClasses.forTokenUnchecked(p.objectType()))))
                .collect(Collectors.toCollection(Sets::newLinkedHashSet));
    }

    private static <S> Collection<PropertyExpression<?, ?, ?>> mandatoryPropertiesNotCached(TypeToken<S> typeToken) {
        MetaClass<S> metaClass = MetaClasses.forTokenUnchecked(typeToken);
        return mandatoryProperties(ObjectExpression.arg(metaClass.asType()), metaClass)
                .collect(Collectors.toCollection(Sets::newLinkedHashSet));
    }

    @SuppressWarnings("unchecked")
    private static <S, T> Stream<PropertyExpression<S, ?, ?>> mandatoryProperties(ObjectExpression<S, T> target, MetaClass<T> metaClass) {
        return Streams.fromIterable(metaClass.properties())
                .filter(p -> !p.hasAnnotation(Nullable.class))
                .flatMap(p -> {
                    PropertyExpression<S, T, ?> propertyExpression = PropertyExpression.ofObject(target, p);
                    Stream<PropertyExpression<S, ?, ?>> stream = (Stream<PropertyExpression<S, ?, ?>>) Optional.of(p.type())
                            .filter(PropertyMetas::hasMetaClass)
                            .map(MetaClasses::forTokenUnchecked)
                            .map(meta -> mandatoryProperties(propertyExpression, (MetaClass)meta))
                            .orElseGet(Stream::empty);
                    return Stream.concat(Stream.of(propertyExpression), stream);
                });
    }

    @SuppressWarnings("unchecked")
    public static <S, T, V> Stream<PropertyExpression<S, ?, ?>> parentProperties(PropertyExpression<S, T, V> property) {
        return ((Collection<PropertyExpression<S, ?, ?>>)(Collection<?>)parentProperties.computeIfAbsent(property, p -> parentPropertiesNonCached(property).collect(Collectors.toList())))
                .stream();
    }

    private static <S, T, V> Stream<PropertyExpression<S, ?, ?>> parentPropertiesNonCached(PropertyExpression<S, T, V> property) {
        return (property.target() instanceof PropertyExpression)
                ? Stream.concat(Stream.of(property), parentProperties((PropertyExpression<S, ?, ?>)property.target()))
                : Stream.of(property);
    }
}

