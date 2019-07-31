package com.slimgears.rxrepo.mongodb.codecs;

import com.slimgears.rxrepo.mongodb.ReferencedObjectResolver;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetaClassCodecProvider implements CodecProvider {
    private final Map<Class, Codec<?>> codecRegistryMap = new ConcurrentHashMap<>();
    private final ReferencedObjectResolver objectResolver;

    private MetaClassCodecProvider(ReferencedObjectResolver objectResolver) {
        this.objectResolver = objectResolver;
    }

    public static CodecProvider create(ReferencedObjectResolver objectResolver) {
        return new MetaClassCodecProvider(objectResolver);
    }

    public static CodecProvider createEmbedded() {
        return new MetaClassCodecProvider(null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry codecRegistry) {
        if (HasMetaClass.class.isAssignableFrom(clazz)) {
            return (Codec<T>)codecRegistryMap.computeIfAbsent(clazz, cls -> forType(cls, codecRegistry));
        }
        return null;
    }

    private <T extends HasMetaClass<T>> Codec<T> forType(Class<T> cls, CodecRegistry codecRegistry) {
        return MetaClassCodec.create(cls, codecRegistry, objectResolver);
    }
}
