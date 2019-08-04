package com.slimgears.rxrepo.encoding.codecs;

import com.google.auto.service.AutoService;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.encoding.*;
import com.slimgears.rxrepo.expressions.internal.MoreTypeTokens;
import com.slimgears.util.reflect.TypeTokens;

@SuppressWarnings("UnstableApiUsage")
@AutoService(MetaCodecProvider.Module.class)
public class EnumCodecModule implements MetaCodecProvider.Module {
    @SuppressWarnings("unchecked")
    @Override
    public MetaCodecProvider create() {
        return MetaCodecs.builder()
                .add(MoreTypeTokens::isEnum, type -> codecForEnum((TypeToken)type))
                .build();
    }

    private <E extends Enum<E>> MetaCodec<E> codecForEnum(TypeToken<E> type) {
        return MetaCodecs.fromAdapter(MetaWriter::writeString, E::name, MetaReader::readString, str -> Enum.valueOf(TypeTokens.asClass(type), str));
    }
}
