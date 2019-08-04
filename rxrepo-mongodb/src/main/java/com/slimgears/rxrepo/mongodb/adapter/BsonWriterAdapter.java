package com.slimgears.rxrepo.mongodb.adapter;

import com.slimgears.rxrepo.encoding.MetaWriter;
import org.bson.BsonBinary;
import org.bson.BsonWriter;

public class BsonWriterAdapter implements MetaWriter {
    private final BsonWriter writer;

    private BsonWriterAdapter(BsonWriter writer) {
        this.writer = writer;
    }

    static MetaWriter forBsonWriter(BsonWriter writer) {
        return new BsonWriterAdapter(writer);
    }

    @Override
    public MetaWriter writeBeginObject() {
        writer.writeStartDocument();
        return this;
    }

    @Override
    public MetaWriter writeEndObject() {
        writer.writeEndDocument();
        return this;
    }

    @Override
    public MetaWriter writeBeginArray() {
        writer.writeStartArray();
        return this;
    }

    @Override
    public MetaWriter writeEndArray() {
        writer.writeEndArray();
        return this;
    }

    @Override
    public MetaWriter writeName(String name) {
        writer.writeName(name);
        return this;
    }

    @Override
    public MetaWriter writeLong(long value) {
        writer.writeInt64(value);
        return this;
    }

    @Override
    public MetaWriter writeInt(int value) {
        writer.writeInt32(value);
        return this;
    }

    @Override
    public MetaWriter writeShort(short value) {
        return writeInt(value);
    }

    @Override
    public MetaWriter writeFloat(float value) {
        return writeDouble(value);
    }

    @Override
    public MetaWriter writeDouble(double value) {
        writer.writeDouble(value);
        return this;
    }

    @Override
    public MetaWriter writeBoolean(boolean value) {
        writer.writeBoolean(value);
        return this;
    }

    @Override
    public MetaWriter writeString(String value) {
        writer.writeString(value);
        return this;
    }

    @Override
    public MetaWriter writeNull() {
        writer.writeNull();
        return this;
    }

    @Override
    public MetaWriter writeBytes(byte[] bytes) {
        writer.writeBinaryData(new BsonBinary(bytes));
        return this;
    }
}
