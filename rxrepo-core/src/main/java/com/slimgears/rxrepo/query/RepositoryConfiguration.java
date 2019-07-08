package com.slimgears.rxrepo.query;

public interface RepositoryConfiguration {
    int retryCount();
    int debounceTimeoutMillis();
    int retryInitialDurationMillis();
}
