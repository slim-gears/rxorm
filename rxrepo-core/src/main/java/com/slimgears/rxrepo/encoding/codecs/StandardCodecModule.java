package com.slimgears.rxrepo.encoding.codecs;

import com.google.auto.service.AutoService;
import com.slimgears.rxrepo.encoding.MetaCodecProvider;
import com.slimgears.rxrepo.encoding.MetaCodecs;
import com.slimgears.rxrepo.encoding.MetaReader;
import com.slimgears.rxrepo.encoding.MetaWriter;
import com.slimgears.util.stream.Safe;

@AutoService(MetaCodecProvider.Module.class)
public class StandardCodecModule implements MetaCodecProvider.Module {
    @Override
    public MetaCodecProvider create() {
        return MetaCodecs.builder()
                .add(Long.class, MetaWriter::writeLong, MetaReader::readLong)
                .add(long.class, MetaWriter::writeLong, MetaReader::readLong)
                .add(Integer.class, MetaWriter::writeInt, MetaReader::readInt)
                .add(int.class, MetaWriter::writeInt, MetaReader::readInt)
                .add(Short.class, MetaWriter::writeShort, MetaReader::readShort)
                .add(short.class, MetaWriter::writeShort, MetaReader::readShort)
                .add(Double.class, MetaWriter::writeDouble, MetaReader::readDouble)
                .add(double.class, MetaWriter::writeDouble, MetaReader::readDouble)
                .add(Float.class, MetaWriter::writeFloat, MetaReader::readFloat)
                .add(float.class, MetaWriter::writeFloat, MetaReader::readFloat)
                .add(Boolean.class, MetaWriter::writeBoolean, MetaReader::readBoolean)
                .add(boolean.class, MetaWriter::writeBoolean, MetaReader::readBoolean)
                .add(String.class, MetaWriter::writeString, MetaReader::readString)
                .add(byte[].class, MetaWriter::writeBytes, MetaReader::readBytes)
                .add(Class.class, MetaCodecs.stringAdapter(Class::getName, Safe.ofFunction(Class::forName)))
                .add(new EnumCodecModule())
                .add(new MapCodec.Provider())
                .add(new IterableCodec.Provider())
                .build();
    }
}
