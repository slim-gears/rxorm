package com.slimgears.rxrepo.sql;

import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;

public interface SchemaProvider {
    <K, T> Completable createOrUpdate(MetaClassWithKey<K, T> metaClass);
    <K, T> String tableName(MetaClassWithKey<K, T> metaClass);
}
