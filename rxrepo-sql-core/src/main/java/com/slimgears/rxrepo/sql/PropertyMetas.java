package com.slimgears.rxrepo.sql;

import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;

public class PropertyMetas {
    public static boolean isReference(PropertyMeta<?, ?> propertyMeta) {
        return propertyMeta.type().is(HasMetaClassWithKey.class::isAssignableFrom);
    }
}
