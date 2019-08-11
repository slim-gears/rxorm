package com.slimgears.rxrepo.mongodb.adapter;

import com.mongodb.DBRefCodecProvider;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

public class StandardCodecs {
    public static CodecRegistry registry() {
        return CodecRegistries.fromProviders(
                new ValueCodecProvider(),
                new DocumentCodecProvider(),
                new BsonValueCodecProvider(),
                new DBRefCodecProvider(),
                new MetaCodecAdapter.Provider());
    }
}
