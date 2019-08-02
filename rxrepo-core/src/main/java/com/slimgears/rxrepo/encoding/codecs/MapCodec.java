package com.slimgears.rxrepo.encoding.codecs;

import com.google.common.collect.ImmutableMap;
import com.slimgears.rxrepo.encoding.*;
import com.slimgears.util.reflect.TypeToken;

import java.util.Map;

public class MapCodec<K, V> implements MetaCodec<Map<K, V>> {
    private final TypeToken<K> keyType;
    private final TypeToken<V> valueType;

    private MapCodec(TypeToken<K> keyType, TypeToken<V> valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public void encode(MetaContext.Writer context, Map<K, V> map) {
        MetaCodec<K> keyCodec = getKeyCodec(context);
        MetaCodec<V> valueCodec = context.codecProvider().resolve(valueType);
        MetaWriter writer = context.writer();
        writer.writeBeginDocument();
        map.forEach((key, value) -> {
            keyCodec.encode(context, key);
            valueCodec.encode(context, value);
        });
        writer.writeEndDocument();
    }

    @Override
    public Map<K, V> decode(MetaContext.Reader context) {
        MetaCodec<K> keyCodec = getKeyCodec(context);
        MetaCodec<V> valueCodec = context.codecProvider().resolve(valueType);
        MetaReader reader = context.reader();
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        reader.readBeginObject();
        while (!reader.isAt(MetaElementType.EndObject)) {
            K key = keyCodec.decode(context);
            V value = valueCodec.decode(context);
            builder.put(key, value);
        }
        reader.readEndObject();
        return builder.build();
    }

    private MetaCodec<K> getKeyCodec(MetaContext context) {
        return keyType.asClass() == String.class
                ? keyAsNameCodec()
                : context.codecProvider().resolve(keyType);
    }

    private MetaCodec<K> keyAsNameCodec() {
        return new MetaCodec<K>() {
            @Override
            public void encode(MetaContext.Writer context, K value) {
                context.writer().writeName(value.toString());
            }

            @SuppressWarnings("unchecked")
            @Override
            public K decode(MetaContext.Reader context) {
                return (K)context.reader().readName();
            }
        };
    }

    public static class Provider implements MetaCodecProvider {
        @SuppressWarnings("unchecked")
        @Override
        public <T> MetaCodec<T> tryResolve(TypeToken<T> type) {
            return type.is(Map.class::isAssignableFrom)
                    ? (MetaCodec<T>)new MapCodec<>(type.typeArguments()[0], type.typeArguments()[1])
                    : null;
        }
    }
}
