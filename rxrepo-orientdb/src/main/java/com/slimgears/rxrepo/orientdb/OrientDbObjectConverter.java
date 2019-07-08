package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.PropertyMeta;

import java.util.*;
import java.util.stream.Collectors;

class OrientDbObjectConverter {
    static Object[] toOrientDbObjects(Object[] objects) {
        Object[] newArgs = new Object[objects.length];
        for (int i = 0; i < objects.length; ++i) {
            newArgs[i] = toOrientDbObject(objects[i]);
        }
        return newArgs;
    }

    @SuppressWarnings("unchecked")
    private static Object toOrientDbObject(Object obj) {
        if (obj instanceof Collection) {
            return convertCollection((Collection<?>)obj);
        } else if (obj instanceof Map) {
            return convertMap((Map<?, ?>)obj);
        } else if (!(obj instanceof HasMetaClass)) {
            return obj;
        }

        HasMetaClass<?> hasMetaClass = (HasMetaClass)obj;
        MetaClass<?> metaClass = hasMetaClass.metaClass();
        OElement oElement = new ODocument(OrientDbSchemaProvider.toClassName(metaClass));
        metaClass.properties().forEach(p -> {
            oElement.setProperty(p.name(), toOrientDbObject(((PropertyMeta)p).getValue(obj)));
            if (p.type().is(HasMetaClass.class::isAssignableFrom) && !p.type().is(HasMetaClassWithKey.class::isAssignableFrom)) {
                Optional.ofNullable(((PropertyMeta)p).getValue(obj))
                        .map(Object::toString)
                        .ifPresent(str -> oElement.setProperty(p.name() + "AsString", str));
            }
        });

        return oElement;
    }

    private static Map<?, ?> convertMap(Map<?, ?> map) {
        return map.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> toOrientDbObject(e.getKey()), e -> toOrientDbObject(e.getValue())));
    }

    private static Collection<?> convertCollection(Collection<?> collection) {
        if (collection instanceof List) {
            return collection.stream().map(OrientDbObjectConverter::toOrientDbObject).collect(Collectors.toList());
        } else if (collection instanceof Set) {
            return collection.stream().map(OrientDbObjectConverter::toOrientDbObject).collect(Collectors.toSet());
        }
        return collection;
    }
}
