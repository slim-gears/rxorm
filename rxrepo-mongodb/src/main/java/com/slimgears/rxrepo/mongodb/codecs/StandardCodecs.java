package com.slimgears.rxrepo.mongodb.codecs;

import com.google.auto.service.AutoService;
import com.mongodb.DBRefCodecProvider;
import org.bson.codecs.*;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

@AutoService(CodecProvider.class)
public class StandardCodecs implements CodecProvider {
    private final static CodecProvider provider = Codecs
            .providerBuilder()
            .providers(
                    new ValueCodecProvider(),
                    new IterableCodecProvider(),
                    new DocumentCodecProvider(),
                    new BsonValueCodecProvider(),
                    new DBRefCodecProvider(),
                    new MapCodecProvider())
            .build();

    @Override
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        return provider.get(clazz, registry);
    }
}
