package com.slimgears.rxrepo.mem;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.test.AbstractRepositoryTest;
import com.slimgears.rxrepo.util.SchedulingProvider;

public class MemoryQueryProviderTest extends AbstractRepositoryTest {
    @Override
    protected Repository createRepository(SchedulingProvider schedulingProvider) {
        return MemoryRepository.create();
    }
}
