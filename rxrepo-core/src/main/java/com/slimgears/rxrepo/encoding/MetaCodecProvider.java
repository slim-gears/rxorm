package com.slimgears.rxrepo.encoding;

import com.google.common.reflect.TypeToken;

import java.util.Optional;

@SuppressWarnings("UnstableApiUsage")
public interface MetaCodecProvider {
    <T> MetaCodec<T> tryResolve(TypeToken<T> type);

    default <T> MetaCodec<T> resolve(Class<T> type) {
        return resolve(TypeToken.of(type));
    }

    default <T> MetaCodec<T> resolve(TypeToken<T> type) {
        return Optional
                .ofNullable(MetaCodecs.<T>tryFindCodec(this, type))
                .orElseThrow(() -> new MetaCodecException("Could not find codec for type: " + type));
    }

    interface Module {
        MetaCodecProvider create();
    }
}
