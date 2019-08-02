package com.slimgears.rxrepo.encoding;

public interface MetaReader {
    MetaElementType currentElement();
    void readBeginObject();
    void readEndObject();
    void readBeginArray();
    void readEndArray();
    String readName();
    long  readLong();
    int readInt();
    short readShort();
    float readFloat();
    double readDouble();
    boolean readBoolean();
    String readString();
    void readNull();
    byte[] readBytes();
    void skipValue();

    default boolean isFinished() {
        return currentElement() == MetaElementType.End;
    }

    default boolean isAt(MetaElementType type) {
        return currentElement() == type;
    }

    default Object readValue() {
        return MetaReaders.readValue(this);
    }
}
