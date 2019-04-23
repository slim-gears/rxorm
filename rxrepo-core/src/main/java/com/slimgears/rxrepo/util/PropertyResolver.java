package com.slimgears.rxrepo.util;

import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;

public interface PropertyResolver {
    Iterable<String> propertyNames();
    Object getProperty(String name, Class type);
    Object getKey(Class<?> keyClass);

    @SuppressWarnings("unchecked")
    default <V> V getProperty(PropertyMeta<?, V> propertyMeta) {
        Object value = getProperty(propertyMeta.name(), propertyMeta.type().asClass());
        return (value instanceof PropertyResolver)
                ? ((PropertyResolver)value).toObject(propertyMeta.type())
                : (V)value;
    }

    static PropertyResolver empty() {
        return PropertyResolvers.empty();
    }

    default <T extends HasMetaClass<T>> T toObject(MetaClass<T> metaClass) {
        return PropertyResolvers.toObject(this, metaClass);
    }

    default PropertyResolver mergeWith(PropertyResolver propertyResolver) {
        return PropertyResolvers.merge(propertyResolver, this);
    }

    @SuppressWarnings("unchecked")
    default <T> T toObject(TypeToken<? extends T> typeToken) {
        return (T)toObject(MetaClasses.forToken((TypeToken)typeToken));
    }

    static <T extends HasMetaClass<T>> PropertyResolver fromObject(T obj) {
        return PropertyResolvers.fromObject(obj);
    }
}
