package com.slimgears.rxrepo.util;

import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.stream.Streams;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("WeakerAccess")
public class PropertyExpressions {
    public static String toPath(PropertyExpression<?, ?, ?> propertyExpression) {
        if (propertyExpression.target() instanceof PropertyExpression) {
            return toPath((PropertyExpression<?, ?, ?>)propertyExpression.target()) + "." + propertyExpression.property().name();
        } else {
            return propertyExpression.property().name();
        }
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
        MetaClass<T> metaClass = MetaClasses.forTokenUnchecked(target.objectType());
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
}
