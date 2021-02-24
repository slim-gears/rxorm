package com.slimgears.rxrepo.query;

import com.google.common.collect.ImmutableList;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.UseAutoValueAnnotator;
import com.slimgears.util.autovalue.annotations.UseBuilderExtension;

import java.util.Arrays;

@AutoValuePrototype(pattern = "(.*)Model")
@UseAutoValueAnnotator
@UseBuilderExtension
public interface RepositoryConfigModel {
    int retryCount();
    int bufferDebounceTimeoutMillis();
    int aggregationDebounceTimeMillis();
    int retryInitialDurationMillis();
}
