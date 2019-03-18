package com.slimgears.util.repository.query;

import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

public interface Repository {
    <K, T extends HasMetaClassWithKey<K, T>> EntitySet<K, T> entities(MetaClassWithKey<K, T> meta);

    static Repository fromProvider(QueryProvider provider) {
        return new DefaultRepository(provider);
    }
}
