package com.slimgears.rxrepo.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.expressions.internal.MoreTypeTokens;
import com.slimgears.util.autovalue.annotations.*;
import com.slimgears.util.generic.ScopedInstance;
import com.slimgears.util.stream.Lazy;
import com.slimgears.util.stream.Safe;
import com.slimgears.util.stream.Streams;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PropertyResolvers {
    private final static ScopedInstance<Set<PropertyMeta<?, ?>>> requiredProperties = ScopedInstance.create();

    static <T> T toObject(PropertyResolver resolver, MetaClass<T> metaClass) {
        BuilderPrototype<T, ?> builder = metaClass.createBuilder();
        Streams.fromIterable(metaClass.properties())
                .filter(PropertyResolvers::isRequiredProperty)
                .map(prop -> PropertyValue.toValue(resolver, prop))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(pv -> pv.set(builder));
        return builder.build();
    }

    static <T> T withProperties(Set<PropertyMeta<?, ?>> properties, Callable<T> callable) {
        return requiredProperties.withScope(properties, callable);
    }

    public static <S, T> T withProperties(ImmutableSet<PropertyExpression<S, ?, ?>> properties, Callable<T> callable) {
        return properties != null && !properties.isEmpty()
            ? withProperties(properties.stream()
                .flatMap(p -> Stream.concat(Stream.of(p), PropertyExpressions.parentProperties(p)))
                .map(PropertyExpression::property)
                .collect(Collectors.toSet()), callable)
            : Safe.ofCallable(callable).get();
    }

    private static boolean isRequiredProperty(PropertyMeta<?, ?> prop) {
        return Optional.ofNullable(requiredProperties.current())
                .map(set -> set.contains(prop) || PropertyMetas.isMandatory(prop))
                .orElse(true);
    }

    static PropertyResolver empty() {
        return new PropertyResolver() {
            @Override
            public Iterable<String> propertyNames() {
                return Collections.emptyList();
            }

            @Override
            public Object getProperty(String name, Class type) {
                return null;
            }
        };
    }

    static PropertyResolver merge(PropertyResolver... propertyResolvers) {
        Lazy<Iterable<String>> propertyNames = Lazy.of(() -> Arrays
            .stream(propertyResolvers)
            .filter(Objects::nonNull)
            .flatMap(pr -> Streams.fromIterable(pr.propertyNames()))
            .collect(Collectors.toSet()));

        PropertyResolver resolver = new PropertyResolver() {
            @Override
            public Iterable<String> propertyNames() {
                return propertyNames.get();
            }

            @Override
            public Object getProperty(String name, Class type) {
                return Arrays.stream(propertyResolvers)
                        .filter(Objects::nonNull)
                        .map(pr -> pr.getProperty(name, type))
                        .filter(Objects::nonNull)
                        .findFirst()
                        .orElse(null);
            }
        };
        return resolver.cache();
    }

    private static <T extends HasMetaClass<T>> PropertyResolver fromObject(T obj) {
        return fromObject(obj.metaClass(), obj);
    }

    static <T> PropertyResolver fromObject(MetaClass<T> metaClass, T obj) {
        if (obj == null) {
            return empty();
        }

        List<String> propertyNames = Streams
                .fromIterable(metaClass.properties())
                .map(PropertyMeta::name)
                .collect(Collectors.toList());

        return new PropertyResolver() {
            @Override
            public Iterable<String> propertyNames() {
                return propertyNames;
            }

            @Override
            public Object getProperty(String name, Class type) {
                return Optional.ofNullable(metaClass.getProperty(name))
                        .map(p -> fromValue(p.type(), p.getValue(obj)))
                        .orElse(null);
            }
            @SuppressWarnings("unchecked")
            @Override
            public <_T> _T toObject(MetaClass<_T> targetMeta) {
                return metaClass.equals(targetMeta)
                    ? (_T)obj
                    : PropertyResolvers.toObject(this, targetMeta);
            }
        }.cache();
    }

    private static <V> Object fromValue(TypeToken<?> type, V value) {
        if (value instanceof HasMetaClass) {
            //noinspection unchecked
            return PropertyResolvers.fromObject((HasMetaClass)value);
        } else if (value instanceof Iterable) {
            TypeToken<?> elementType = type.resolveType(Iterable.class.getTypeParameters()[0]);
            if (elementType.isSubtypeOf(HasMetaClass.class)) {
                Stream<?> stream = Streams.fromIterable((Iterable<?>)value)
                        .map(val -> fromValue(elementType, val));
                if (value instanceof Set) {
                    return stream.collect(ImmutableSet.toImmutableSet());
                } else if (value instanceof List) {
                    return stream.collect(ImmutableList.toImmutableList());
                }
            }
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private static <V> V toValue(TypeToken<V> type, Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Iterable && type.isSubtypeOf(Iterable.class)) {
            TypeToken<?> elementType = MoreTypeTokens.elementType((TypeToken)type);
            Stream<?> objects = elementType.isSubtypeOf(HasMetaClass.class)
                    ? Streams.fromIterable((Iterable<?>)value)
                    .flatMap(Streams.ofType(PropertyResolver.class))
                    .map(pr -> pr.toObject(elementType))
                    : Streams.fromIterable((Iterable<?>)value);

            if (value instanceof List) {
                return (V)objects.collect(ImmutableList.toImmutableList());
            } else if (value instanceof Set) {
                return (V)objects.collect(ImmutableSet.toImmutableSet());
            } else {
                throw new RuntimeException("Not supported iterable type: " + value.getClass());
            }

        } else if (value instanceof Map && type.isSubtypeOf(Map.class)) {
            TypeToken<?> keyType = MoreTypeTokens.keyType((TypeToken)type);
            TypeToken<?> valType = MoreTypeTokens.valueType((TypeToken)type);
            return (V)((Map<?,?>)value)
                    .entrySet()
                    .stream()
                    .collect(ImmutableMap.toImmutableMap(
                            val -> toValue(keyType, val.getKey()),
                            val -> toValue(valType, val.getValue())));
        } else if (type.getRawType().isInstance(value)) {
            return (V)value;
        } else if ((value instanceof PropertyResolver) && type.isSubtypeOf(HasMetaClass.class)) {
            return (V)((PropertyResolver) value).toObject((TypeToken)type);
        } else {
            throw new RuntimeException("Cannot convert value: " + value + " to type: " + type);
        }
    }

    private static class PropertyValue<T, V> {
        final PropertyMeta<T, V> property;
        final V value;

        void set(MetaBuilder<T> builder) {
            property.setValue(builder, PropertyResolvers.toValue(property.type(), value));
        }

        private PropertyValue(PropertyMeta<T, V> property, V value) {
            this.property = property;
            this.value = value;
        }

        static <T, V> Optional<PropertyValue<T, V>> toValue(PropertyResolver resolver, PropertyMeta<T, V> prop) {
            return Optional.ofNullable(resolver.getProperty(prop))
                    .map(val -> new PropertyValue<>(prop, val));
        }
    }
}
