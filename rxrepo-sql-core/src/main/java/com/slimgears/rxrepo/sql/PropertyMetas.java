package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.annotations.Indexable;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;

import java.util.Optional;

public class PropertyMetas {
    static boolean isReference(PropertyMeta<?, ?> propertyMeta) {
        return propertyMeta.type().is(HasMetaClassWithKey.class::isAssignableFrom);
    }

    private static boolean hasMetaClass(PropertyMeta<?, ?> propertyMeta) {
        return HasMetaClass.class.isAssignableFrom(propertyMeta.type().asClass());
    }

    public static boolean isIndexableByString(PropertyMeta<?, ?> propertyMeta) {
        return (isKey(propertyMeta) && hasMetaClass(propertyMeta)) ||
                Optional.ofNullable(propertyMeta.getAnnotation(Indexable.class))
                        .map(Indexable::asString)
                        .orElse(false);
    }

    private static boolean isKey(PropertyMeta<?, ?> propertyMeta) {
        MetaClass metaClass = propertyMeta.declaringType();
        return metaClass instanceof MetaClassWithKey && ((MetaClassWithKey)metaClass).keyProperty() == propertyMeta;
    }
}
