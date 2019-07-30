package com.slimgears.rxrepo.mongodb.codecs;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

public class ImmutableListCodec implements Codec<ImmutableList> {
    private final Codec<Iterable> iterableCodec;

    private ImmutableListCodec(CodecRegistry codecRegistry) {
        this.iterableCodec = codecRegistry.get(Iterable.class);
    }

    @Override
    public ImmutableList decode(BsonReader reader, DecoderContext decoderContext) {
        Iterable iterable = iterableCodec.decode(reader, decoderContext);
        return ImmutableList.copyOf(iterable);
    }

    @Override
    public void encode(BsonWriter writer, ImmutableList value, EncoderContext encoderContext) {
        iterableCodec.encode(writer, value, encoderContext);
    }

    @Override
    public Class<ImmutableList> getEncoderClass() {
        return ImmutableList.class;
    }

    @AutoService(CodecProvider.class)
    public static class Provider implements CodecProvider {
        @SuppressWarnings("unchecked")
        @Override
        public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
            return (clazz == ImmutableList.class)
                    ? (Codec<T>)new ImmutableListCodec(registry)
                    : null;
        }
    }
}
