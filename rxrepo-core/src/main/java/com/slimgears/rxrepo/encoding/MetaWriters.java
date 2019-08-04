package com.slimgears.rxrepo.encoding;

@SuppressWarnings("WeakerAccess")
public class MetaWriters {
    public static MetaWriter writeValue(MetaWriter writer, Object value) {
        if (value == null) {
            writer.writeNull();
            return writer;
        }
        if (value instanceof Integer) {
            writer.writeInt((Integer)value);
        } else if (value instanceof Long) {
            writer.writeLong((Long)value);
        } else if (value instanceof Short) {
            writer.writeShort((Short)value);
        } else if (value instanceof Float) {
            writer.writeFloat((Float)value);
        } else if (value instanceof Double) {
            writer.writeDouble((Double)value);
        } else if (value instanceof String) {
            writer.writeString((String)value);
        } else if (value instanceof Boolean) {
            writer.writeBoolean((Boolean)value);
        } else if (value instanceof byte[]) {
            writer.writeBytes((byte[])value);
        } else {
            throw new IllegalStateException("Unrecognized value type: " + value.getClass().getName());
        }

        return writer;
    }
}
