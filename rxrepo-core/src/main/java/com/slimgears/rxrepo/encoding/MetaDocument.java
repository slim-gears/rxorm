package com.slimgears.rxrepo.encoding;

import com.slimgears.util.reflect.TypeToken;

import java.util.Map;

public interface MetaDocument {
    Map<String, Object> asMap();
    <V> MetaDocument set(String name, V value);
    <V> V get(String name, TypeToken<V> type);

    static MetaDocument create() {
        return MetaDocuments.create();
    }
}
