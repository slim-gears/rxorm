package com.slimgears.rxrepo.mongodb.adapter;

import com.slimgears.rxrepo.encoding.MetaReader;
import com.slimgears.rxrepo.encoding.MetaWriter;
import org.bson.BsonReader;
import org.bson.BsonWriter;

class BsonAdapter {
    static MetaReader forReader(BsonReader reader) {
        return BsonReaderAdapter.forBsonReader(reader);
    }

    static MetaWriter forWriter(BsonWriter writer) {
        return BsonWriterAdapter.forBsonWriter(writer);
    }
}
