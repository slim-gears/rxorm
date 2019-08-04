package com.slimgears.rxrepo.encoding;

public interface MetaWriter {
    MetaWriter writeBeginObject();
    MetaWriter writeEndObject();
    MetaWriter writeBeginArray();
    MetaWriter writeEndArray();
    MetaWriter writeName(String name);
    MetaWriter writeLong(long value);
    MetaWriter writeInt(int value);
    MetaWriter writeShort(short value);
    MetaWriter writeFloat(float value);
    MetaWriter writeDouble(double value);
    MetaWriter writeBoolean(boolean value);
    MetaWriter writeString(String value);
    MetaWriter writeNull();
    MetaWriter writeBytes(byte[] bytes);

    default MetaWriter writeValue(Object object) {
        return MetaWriters.writeValue(this, object);
    }
}
