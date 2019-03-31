package com.slimgears.rxrepo.util;

import com.slimgears.util.autovalue.annotations.BuilderPrototype;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.MetaBuilder;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.stream.Streams;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.slimgears.util.stream.Optionals.ofType;

public class PropertyResolvers {
    public static <T extends HasMetaClass<T>> T toObject(PropertyResolver resolver, MetaClass<T> metaClass) {
        BuilderPrototype<T, ?> builder = metaClass.createBuilder();
        Streams.fromIterable(resolver.propertyNames())
                .map(metaClass::getProperty)
                .filter(Objects::nonNull)
                .flatMap(PropertyValue.toValue(resolver))
                .forEach(pv -> pv.set(builder));
        return builder.build();
    }

    public static PropertyResolver empty() {
        return new PropertyResolver() {
            @Override
            public Iterable<String> propertyNames() {
                return Collections.emptyList();
            }

            @Override
            public Object getProperty(String name, Class type) {
                return null;
            }

            @Override
            public Object getKey(Class keyClass) {
                return null;
            }
        };
    }

    public static <T extends HasMetaClass<T>> PropertyResolver fromObject(T obj) {
        if (obj == null) {
            return empty();
        }

        MetaClass<T> metaClass = obj.metaClass();
        List<String> propertyNames = Streams
                .fromIterable(metaClass.properties())
                .map(PropertyMeta::name)
                .collect(Collectors.toList());

        Optional<String> keyProperty = Optional
                .of(metaClass)
                .flatMap(ofType(MetaClassWithKey.class))
                .map(MetaClassWithKey::keyProperty)
                .map(PropertyMeta::name);

        return new PropertyResolver() {
            @Override
            public Iterable<String> propertyNames() {
                return propertyNames;
            }

            @Override
            public Object getProperty(String name, Class type) {
                PropertyMeta<T, ?> property = metaClass.getProperty(name);
                return fromValue(property.type(), property.getValue(obj));
            }

            @Override
            public Object getKey(Class keyClass) {
                return keyProperty
                        .map(name -> getProperty(name, keyClass))
                        .orElse(null);
            }
        };
    }

    private static <V> Object fromValue(TypeToken<?> type, V value) {
        if (value instanceof HasMetaClass) {
            //noinspection unchecked
            return PropertyResolvers.fromObject((HasMetaClass)value);
        } else if (value instanceof Iterable) {
            TypeToken<?> elementType = typeParam(type, 0);
            if (elementType.is(HasMetaClass.class::isAssignableFrom)) {
                Stream<?> stream = Streams.fromIterable((Iterable<?>)value)
                        .map(val -> fromValue(elementType, val));
                if (value instanceof Set) {
                    return stream.collect(Collectors.toSet());
                } else if (value instanceof List) {
                    return stream.collect(Collectors.toList());
                }
            }
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private static <V> V toValue(TypeToken<V> type, Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Iterable) {
            TypeToken<?> elementType = typeParam(type, 0);
            Stream<?> objects = Streams.fromIterable((Iterable<?>)value)
                    .flatMap(Streams.ofType(PropertyResolver.class))
                    .map(pr -> pr.toObject(elementType));
            if (value instanceof List) {
                return (V)objects.collect(Collectors.toList());
            } else if (value instanceof Set) {
                return (V)objects.collect(Collectors.toSet());
            } else {
                throw new RuntimeException("Not supported iterable type: " + value.getClass());
            }

        } else if (value instanceof Map) {
            TypeToken<?> elementType = typeParam(type, 1);
            return (V)((Map<?,?>)value)
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, val -> toValue(elementType, value)));
        } else if (type.asClass().isInstance(value)) {
            return (V)value;
        } else if ((value instanceof PropertyResolver) && HasMetaClass.class.isAssignableFrom(type.asClass())) {
            return (V)((PropertyResolver) value).toObject((TypeToken)type);
        } else {
            throw new RuntimeException("Cannot convert value: " + value + " to type: " + type);
        }
    }

    private static <T, R> TypeToken<R> typeParam(TypeToken<T> token, int index) {
        //noinspection unchecked
        return Optional
                .ofNullable(token.typeArguments())
                .filter(args -> args.length > index)
                .map(args -> (TypeToken<R>)args[index])
                .orElse(null);
    }

    private static class PropertyValue<T, V> {
        final PropertyMeta<T, V> property;
        final V value;

        void set(MetaBuilder<T> builder) {
            property.setValue(builder, value);
        }

        private PropertyValue(PropertyMeta<T, V> property, V value) {
            this.property = property;
            this.value = value;
        }

        static <T, V> Function<PropertyMeta<T, V>, Stream<PropertyValue<T, V>>> toValue(PropertyResolver resolver) {
            return prop -> {
                V value = resolver.getProperty(prop);
                return value != null
                        ? Stream.of(new PropertyValue<>(prop, value))
                        : Stream.empty();
            };
        }
    }
}
