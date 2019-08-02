package com.slimgears.rxrepo.encoding;

import com.slimgears.util.reflect.TypeToken;

import java.util.Map;

public interface MetaDocument {
    Map<String, Object> asMap();
    <V> MetaDocument set(String name, V value);
    <V> V get(String name, TypeToken<V> type);

    default <V> V get(String name, Class<V> cls) {
        return get(name, TypeToken.of(cls));
    }

    static MetaDocument create() {
        return MetaDocuments.create();
    }
}
