package com.slimgears.rxrepo.mongodb.codecs;

import com.mongodb.DBRefCodecProvider;
import com.slimgears.rxrepo.mongodb.adapter.MetaCodecAdapter;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecProvider;

public class StandardCodecs {
    public final static CodecProvider provider = CodecProviders
            .combine(
                    new ValueCodecProvider(),
                    new DocumentCodecProvider(),
                    new BsonValueCodecProvider(),
                    new DBRefCodecProvider(),
                    new MetaCodecAdapter.Provider());
}
