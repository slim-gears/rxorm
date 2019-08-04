package com.slimgears.rxrepo.encoding;

import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.stream.Optionals;

import java.util.Optional;

public interface MetaClassFieldMapper {
    default <T, V> String toFieldName(PropertyMeta<T, V> propertyMeta) {
        return PropertyMetas.isKey(propertyMeta)
                ? keyField(MetaClasses.forTokenWithKeyUnchecked(propertyMeta.declaringType().asType()))
                : propertyMeta.name();
    }

    default <T, V> String toReferenceFieldName(PropertyMeta<T, V> propertyMeta) {
        return toFieldName(propertyMeta);
    }

    default <T, V> PropertyMeta<T, V> fromFieldName(MetaClass<T> metaClass, String name) {
        boolean isKey = Optional.of(metaClass)
                .flatMap(Optionals.ofType(MetaClassWithKey.class))
                .map(mc -> (MetaClassWithKey<?, ?>)mc)
                .map(this::keyField)
                .map(name::equals)
                .orElse(false);
        return isKey
                ? MetaClasses.<V, T>forTokenWithKeyUnchecked(metaClass.asType()).keyProperty()
                : metaClass.getProperty(name);
    }

    default <T, V> PropertyMeta<T, V> fromReferenceFieldName(MetaClass<T> metaClass, String name) {
        return fromFieldName(metaClass, name);
    }

    default String versionField() {
        return "__version";
    }

    default <K, S> String keyField(MetaClassWithKey<K, S> metaClassWithKey) {
        return metaClassWithKey.keyProperty().name();
    }

    default String searchableTextField() {
        return "__text";
    }

    default boolean isReferenceFieldName(String field) {
        return false;
    }
}
