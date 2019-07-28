package com.slimgears.rxrepo.mongodb.codecs;

import com.google.auto.service.AutoService;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

@AutoService(CodecProvider.class)
public class EnumCodecProvider implements CodecProvider {
    @SuppressWarnings("unchecked")
    @Override
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        return clazz.isEnum() ? Codecs.enumCodec((Class)clazz) : null;
    }
}
