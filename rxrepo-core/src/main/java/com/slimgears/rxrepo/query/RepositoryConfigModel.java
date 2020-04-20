package com.slimgears.rxrepo.query;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.UseAutoValueAnnotator;
import com.slimgears.util.autovalue.annotations.UseBuilderExtension;

@AutoValuePrototype(pattern = "(.*)Model")
@UseAutoValueAnnotator
@UseBuilderExtension
public interface RepositoryConfigModel {
    int retryCount();
    int bufferDebounceTimeoutMillis();
    int aggregationDebounceTimeMillis();
    int retryInitialDurationMillis();
}
