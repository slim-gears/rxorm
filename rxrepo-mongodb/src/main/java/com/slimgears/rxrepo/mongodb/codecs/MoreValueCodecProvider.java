package com.slimgears.rxrepo.mongodb.codecs;

import com.google.auto.service.AutoService;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.time.Duration;
import java.util.Date;

@AutoService(CodecProvider.class)
public class MoreValueCodecProvider implements CodecProvider {
    private final static CodecProvider provider = Codecs.providerBuilder()
            .stringCodec(Class.class, Class::getName, Class::forName)
            .longCodec(Date.class, Date::getTime, Date::new)
            .longCodec(Duration.class, Duration::toMillis, Duration::ofMillis)
            .build();

    @Override
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        return provider.get(clazz, registry);
    }
}
