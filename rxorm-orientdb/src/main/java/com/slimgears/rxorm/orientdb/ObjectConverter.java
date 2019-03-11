package com.slimgears.rxorm.orientdb;

import com.orientechnologies.orient.core.record.OElement;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;

import java.util.List;

public interface ObjectConverter {
    <T> T toObject(OElement element, TypeToken<? extends T> type, List<PropertyMeta<T, ?, ?>> properties);
    <T> T toObject(OElement element, MetaClass<T, ?> metaClass, List<PropertyMeta<T, ?, ?>> properties);
    <T> OElement toElement(T object);
}
