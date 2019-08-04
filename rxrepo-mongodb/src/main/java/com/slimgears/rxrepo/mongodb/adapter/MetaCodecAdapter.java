package com.slimgears.rxrepo.mongodb.adapter;

import com.google.auto.service.AutoService;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.encoding.*;
import com.slimgears.util.reflect.TypeTokens;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Optional;

public class MetaCodecAdapter<T> implements Codec<T> {
    private final TypeToken<T> type;
    private final MetaCodec<T> codec;
    private final MetaContext context;

    private MetaCodecAdapter(TypeToken<T> type, MetaCodec<T> codec, MetaContext context) {
        this.type = type;
        this.codec = codec;
        this.context = context;
    }

    @Override
    public T decode(BsonReader reader, DecoderContext decoderContext) {
        return codec.decode(context.ofReader(BsonAdapter.forReader(reader)));
    }

    @Override
    public void encode(BsonWriter writer, T value, EncoderContext encoderContext) {
        codec.encode(context.ofWriter(BsonAdapter.forWriter(writer)), value);
    }

    @Override
    public Class<T> getEncoderClass() {
        return TypeTokens.asClass(type);
    }

    @AutoService(CodecProvider.class)
    public static class Provider implements CodecProvider {
        private final MetaContext context;

        public Provider() {
            context = MetaContexts.create(MetaCodecs.discover(), new MongoFieldMapper());
        }

        public Provider(MetaCodecProvider codecProvider) {
            context = MetaContexts.create(codecProvider, new MongoFieldMapper());
        }

        @Override
        public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
            return Optional.ofNullable(context.codecProvider().tryResolve(TypeToken.of(clazz)))
                    .map(c -> new MetaCodecAdapter<>(TypeToken.of(clazz), c, context))
                    .orElse(null);
        }
    }
}
