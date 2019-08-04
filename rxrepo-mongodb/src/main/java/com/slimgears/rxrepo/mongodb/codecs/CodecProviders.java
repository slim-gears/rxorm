package com.slimgears.rxrepo.mongodb.codecs;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Arrays;
import java.util.Objects;

class CodecProviders {
    static CodecProvider combine(CodecProvider... providers) {
        return new CodecProvider() {
            @Override
            public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
                return Arrays.stream(providers)
                        .map(p -> p.get(clazz, registry))
                        .filter(Objects::nonNull)
                        .findFirst()
                        .orElse(null);
            }
        };
    }
}
