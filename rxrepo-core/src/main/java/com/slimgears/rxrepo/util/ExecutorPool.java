package com.slimgears.rxrepo.util;

import java.util.concurrent.Executor;

public interface ExecutorPool {
    Executor getExecutor();
}
