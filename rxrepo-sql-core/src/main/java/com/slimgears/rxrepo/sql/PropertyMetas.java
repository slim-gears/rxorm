package com.slimgears.rxrepo.sql;

import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;

@SuppressWarnings("WeakerAccess")
public class PropertyMetas {
    public static boolean isReference(PropertyMeta<?, ?> propertyMeta) {
        return isReference(propertyMeta.type());
    }

    public static boolean isEmbedded(PropertyMeta<?, ?> propertyMeta) {
        return isEmbedded(propertyMeta.type());
    }

    public static boolean isReference(TypeToken<?> typeToken) {
        return typeToken.is(HasMetaClassWithKey.class::isAssignableFrom);
    }

    public static boolean isEmbedded(TypeToken<?> typeToken) {
        return hasMetaClass(typeToken) && !isReference(typeToken);
    }

    public static boolean hasMetaClass(TypeToken<?> typeToken) {
        return typeToken.is(HasMetaClass.class::isAssignableFrom);
    }
}
