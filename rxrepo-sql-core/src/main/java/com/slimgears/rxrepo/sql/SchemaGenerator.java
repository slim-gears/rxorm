package com.slimgears.rxrepo.sql;

import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;

public interface SchemaGenerator {
    Completable createDatabase();
    <K, T> Completable createOrUpdate(MetaClassWithKey<K, T> metaClass);
    void clear();

    default <K, T> Completable useTable(MetaClassWithKey<K, T> metaClass) {
        return createDatabase().andThen(createOrUpdate(metaClass));
    }
}
