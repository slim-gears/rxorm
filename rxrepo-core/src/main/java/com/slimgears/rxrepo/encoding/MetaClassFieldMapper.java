package com.slimgears.rxrepo.encoding;

import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;

public interface MetaClassFieldMapper {
    default <T, V> String toFieldName(PropertyMeta<T, V> propertyMeta) {
        return propertyMeta.name();
    }

    default <T, V> String toReferenceFieldName(PropertyMeta<T, V> propertyMeta) {
        return toFieldName(propertyMeta);
    }

    default <T, V> PropertyMeta<T, V> fromFieldName(MetaClass<T> metaClass, String name) {
        return metaClass.getProperty(name);
    }

    default <T, V> PropertyMeta<T, V> fromReferenceFieldName(MetaClass<T> metaClass, String name) {
        return fromFieldName(metaClass, name);
    }

    default String versionField() {
        return "__version";
    }

    default <K, S> String keyField(MetaClassWithKey<K, S> metaClassWithKey) {
        return toFieldName(metaClassWithKey.keyProperty());
    }

    default String searchableTextField() {
        return "__text";
    }

    default boolean isReferenceFieldName(String field) {
        return false;
    }
}
