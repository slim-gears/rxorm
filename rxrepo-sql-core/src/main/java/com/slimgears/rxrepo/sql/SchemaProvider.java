package com.slimgears.rxrepo.sql;

import com.slimgears.util.autovalue.annotations.MetaClass;
import io.reactivex.Completable;

public interface SchemaProvider {
    String databaseName();
    <T> Completable createOrUpdate(MetaClass<T> metaClass);
    <T> String tableName(MetaClass<T> metaClass);
    void clear();
}
