package com.slimgears.rxrepo.encoding;

public class MetaContexts {
    public static MetaContext create(MetaCodecProvider codecProvider, MetaClassFieldMapper mapper) {
        return new MetaContext() {
            @Override
            public MetaClassFieldMapper fieldMapper() {
                return mapper;
            }

            @Override
            public MetaCodecProvider codecProvider() {
                return codecProvider;
            }
        };
    }

    public static MetaContext create(MetaCodecProvider provider) {
        return create(provider, defaultFieldMapper());
    }

    @SuppressWarnings("WeakerAccess")
    public static MetaClassFieldMapper defaultFieldMapper() {
        return new MetaClassFieldMapper() {};
    }
}
