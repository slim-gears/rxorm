package com.slimgears.rxrepo.mongodb.codecs;

import com.slimgears.util.generic.ScopedInstance;
import org.bson.Transformer;

import java.util.concurrent.Callable;

public class ContextValueTransformer implements Transformer {
    private final static ScopedInstance<Transformer> transformer = ScopedInstance.create(obj -> obj);

    static Transformer current() {
        return ContextValueTransformer.transformer.current();
    }

    static <T> T withTransformer(Transformer transformer, Callable<? extends T> action) {
        return ContextValueTransformer.transformer.withScope(transformer, action);
    }

    @Override
    public Object transform(Object objectToTransform) {
        return transformer.current().transform(objectToTransform);
    }
}
