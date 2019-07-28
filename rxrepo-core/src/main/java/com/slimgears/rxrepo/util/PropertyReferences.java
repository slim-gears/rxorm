package com.slimgears.rxrepo.util;

import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.stream.Streams;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PropertyReferences {
    private final static Map<MetaClass<?>, List<PropertyReference>> propertyReferencesCache = new ConcurrentHashMap<>();

    private static PropertyReference fromProperty(String referencePath, PropertyMeta<?, ?> propertyMeta) {
        return new PropertyReference() {
            @Override
            public String referencePath() {
                return referencePath;
            }

            @Override
            public PropertyMeta<?, ?> property() {
                return propertyMeta;
            }
        };
    }

    public static List<PropertyReference> forMetaClass(MetaClass<?> metaClass) {
        return propertyReferencesCache.computeIfAbsent(metaClass, PropertyReferences::findReferences);
    }

    private static List<PropertyReference> findReferences(MetaClass<?> metaClass) {
        return findReferences("", metaClass, new HashSet<>()).collect(Collectors.toList());
    }

    private static Stream<PropertyReference> findReferences(String prefixPath, MetaClass<?> metaClass, Set<MetaClass<?>> visitedMetaClasses) {
        return visitedMetaClasses.add(metaClass)
                ? Streams.fromIterable(metaClass.properties())
                .filter(PropertyMetas::hasMetaClass)
                .flatMap(p -> findReferencesForProperty(prefixPath, p, visitedMetaClasses))
                : Stream.empty();
    }

    private static Stream<PropertyReference> findReferencesForProperty(String prefixPath, PropertyMeta<?, ?> propertyMeta, Set<MetaClass<?>> visitedMetaClasses) {
        Stream<PropertyReference> nestedReferences = findReferences(combinePath(prefixPath, propertyMeta.name()), MetaClasses.forTokenUnchecked(propertyMeta.type()), visitedMetaClasses);
        return PropertyMetas.isReference(propertyMeta)
                ? Stream.concat(Stream.of(fromProperty(prefixPath, propertyMeta)), nestedReferences)
                : nestedReferences;
    }

    private static String combinePath(String head, String tail) {
        return head + tail + ".";
    }
}
