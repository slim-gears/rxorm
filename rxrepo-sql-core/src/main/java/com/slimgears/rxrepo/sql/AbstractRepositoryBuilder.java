package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.query.RepositoryConfig;
import com.slimgears.rxrepo.query.RepositoryConfigModelBuilder;
import com.slimgears.rxrepo.util.SchedulingProvider;
import com.slimgears.util.stream.Lazy;

import java.util.function.Supplier;

public abstract class AbstractRepositoryBuilder<_B extends AbstractRepositoryBuilder<_B>> implements RepositoryConfigModelBuilder<_B> {
    protected final RepositoryConfig.Builder configBuilder = RepositoryConfig.builder();
    protected Lazy<SchedulingProvider> schedulingProvider = Lazy.of(SchedulingProvider::empty);

    @SuppressWarnings("unchecked")
    protected _B self() {
        return (_B)this;
    }

    @Override
    public _B retryCount(int value) {
        configBuilder.retryCount(value);
        return self();
    }

    @Override
    public _B bufferDebounceTimeoutMillis(int value) {
        configBuilder.bufferDebounceTimeoutMillis(value);
        return self();
    }

    @Override
    public _B aggregationDebounceTimeMillis(int value) {
        configBuilder.aggregationDebounceTimeMillis(value);
        return self();
    }

    @Override
    public _B retryInitialDurationMillis(int value) {
        configBuilder.retryInitialDurationMillis(value);
        return self();
    }

    public _B schedulingProvider(Supplier<SchedulingProvider> schedulingProvider) {
        this.schedulingProvider = Lazy.of(schedulingProvider);
        return self();
    }

    public _B schedulingProvider(SchedulingProvider schedulingProvider) {
        return schedulingProvider(() -> schedulingProvider);
    }
}
