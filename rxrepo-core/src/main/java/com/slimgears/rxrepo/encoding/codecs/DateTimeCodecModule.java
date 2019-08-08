package com.slimgears.rxrepo.encoding.codecs;

import com.google.auto.service.AutoService;
import com.slimgears.rxrepo.encoding.MetaCodecProvider;
import com.slimgears.rxrepo.encoding.MetaCodecs;

import java.time.Duration;
import java.util.Date;

@AutoService(MetaCodecProvider.Module.class)
public class DateTimeCodecModule implements MetaCodecProvider.Module {
    @Override
    public MetaCodecProvider create() {
        return MetaCodecs.builder()
                .add(Date.class, MetaCodecs.longAdapter(Date::getTime, Date::new))
                .add(Duration.class, MetaCodecs.longAdapter(Duration::toMillis, Duration::ofMillis))
                .build();
    }
}
