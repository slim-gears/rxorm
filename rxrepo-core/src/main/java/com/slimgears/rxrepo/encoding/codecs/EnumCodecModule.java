package com.slimgears.rxrepo.encoding.codecs;

import com.google.auto.service.AutoService;
import com.slimgears.rxrepo.encoding.*;
import com.slimgears.util.reflect.TypeToken;

@AutoService(MetaCodecProvider.Module.class)
public class EnumCodecModule implements MetaCodecProvider.Module {
    @SuppressWarnings("unchecked")
    @Override
    public MetaCodecProvider create() {
        return MetaCodecs.builder()
                .add(TypeToken::isEnum, type -> codecForEnum((TypeToken)type))
                .build();
    }

    private <E extends Enum<E>> MetaCodec<E> codecForEnum(TypeToken<E> type) {
        return MetaCodecs.fromAdapter(MetaWriter::writeString, E::name, MetaReader::readString, str -> Enum.valueOf(type.asClass(), str));
    }
}
