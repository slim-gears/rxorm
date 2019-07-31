package com.slimgears.rxrepo.mongodb.codecs;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Map;

public class ImmutableMapCodec implements Codec<ImmutableMap> {
    private final Codec<Map> mapCodec;

    private ImmutableMapCodec(CodecRegistry codecRegistry) {
        this.mapCodec = codecRegistry.get(Map.class);
    }

    @Override
    public ImmutableMap decode(BsonReader reader, DecoderContext decoderContext) {
        return ImmutableMap.copyOf(mapCodec.decode(reader, decoderContext));
    }

    @Override
    public void encode(BsonWriter writer, ImmutableMap value, EncoderContext encoderContext) {
        mapCodec.encode(writer, value, encoderContext);
    }

    @Override
    public Class<ImmutableMap> getEncoderClass() {
        return ImmutableMap.class;
    }

    @AutoService(CodecProvider.class)
    public static class Provider implements CodecProvider {

        @SuppressWarnings("unchecked")
        @Override
        public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
            return clazz == ImmutableMap.class
                    ? (Codec<T>)new ImmutableMapCodec(registry)
                    : null;
        }
    }
}
