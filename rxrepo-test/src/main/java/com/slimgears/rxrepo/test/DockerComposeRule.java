package com.slimgears.rxrepo.test;

import org.junit.Assume;
import org.junit.rules.ExternalResource;

public class DockerComposeRule extends ExternalResource {
    @Override
    protected void before() throws Throwable {
        Assume.assumeTrue(DockerUtils.isAvailable());
        DockerUtils.start();
    }

    @Override
    protected void after() {
        DockerUtils.stop();
    }
}
