package com.slimgears.rxrepo.mongodb.adapter;

import com.slimgears.rxrepo.encoding.MetaClassFieldMapper;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;

public class MongoFieldMapper implements MetaClassFieldMapper {
    public final static MetaClassFieldMapper instance = new MongoFieldMapper();

    @Override
    public <T, V> String toReferenceFieldName(PropertyMeta<T, V> propertyMeta) {
        return toFieldName(propertyMeta) + "__ref";
    }

    @Override
    public <T, V> PropertyMeta<T, V> fromReferenceFieldName(MetaClass<T> metaClass, String name) {
        return isReferenceFieldName(name)
                ? fromFieldName(metaClass, name.substring(0, name.length() - 5))
                : fromFieldName(metaClass, name);
    }

    @Override
    public boolean isReferenceFieldName(String field) {
        return field.endsWith("__ref");
    }

    @Override
    public <K, S> String keyField(MetaClassWithKey<K, S> metaClassWithKey) {
        return "_id";
    }
}
