package com.slimgears.rxrepo.encoding;

import com.google.common.base.Strings;
import com.slimgears.rxrepo.annotations.Searchable;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.stream.Streams;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public class MetaClassSearchableFields {
    public static <T> Optional<Function<T, String>> searchableTextFromEntity(MetaClass<T> metaClass) {
        return searchableTextFromEntity(metaClass, e -> e, new HashSet<>());
    }

    private static <T, R> Optional<Function<T, String>> searchableTextFromEntity(MetaClass<R> metaClass, Function<T, R> getter, Set<PropertyMeta<?, ?>> visitedProperties) {
        Optional<Function<T, String>> selfFields = searchableTextForMetaClass(metaClass, visitedProperties)
                .map(getter::andThen);

        Optional<Function<T, String>> nestedFields = Streams
                .fromIterable(metaClass.properties())
                .filter(p -> p.type().isSubtypeOf(HasMetaClass.class))
                .filter(visitedProperties::add)
                .map(p -> searchableTextFromProperty(getter, p, visitedProperties))
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .reduce(MetaClassSearchableFields::combine);

        if (selfFields.isPresent() && nestedFields.isPresent()) {
            return Optional.of(combine(selfFields.get(), nestedFields.get()));
        } else if (selfFields.isPresent()) {
            return selfFields;
        } else {
            return nestedFields;
        }
    }

    private static <T, R, V> Optional<Function<T, String>> searchableTextFromProperty(Function<T, R> getter, PropertyMeta<R, V> propertyMeta, Set<PropertyMeta<?, ?>> visitedProperties) {
        Function<T, V> nextGetter = val -> Optional.ofNullable(getter.apply(val)).map(propertyMeta::getValue).orElse(null);
        MetaClass<V> metaClass = MetaClasses.forTokenUnchecked(propertyMeta.type());
        return searchableTextFromEntity(metaClass, nextGetter, visitedProperties);
    }

    private static <T> Function<T, String> combine(Function<T, String> first, Function<T, String> second) {
        return entity -> combineStrings(first.apply(entity), second.apply(entity));
    }

    private static <T> Optional<Function<T, String>> searchableTextForMetaClass(MetaClass<T> metaClass, Set<PropertyMeta<?, ?>> visitedProperties) {
        return Streams
                .fromIterable(metaClass.properties())
                .filter(p -> p.hasAnnotation(Searchable.class))
                .filter(visitedProperties::add)
                .<Function<T, String>>map(p -> (entity -> Optional
                        .ofNullable(entity)
                        .map(p::getValue)
                        .map(Object::toString)
                        .orElse("")))
                .reduce(MetaClassSearchableFields::combine);
    }

    private static String combineStrings(String first, String second) {
        if (Strings.isNullOrEmpty(first)) {
            return second != null ? second : "";
        }
        if (Strings.isNullOrEmpty(second)) {
            return first;
        }
        return first + " " + second;
    }
}
