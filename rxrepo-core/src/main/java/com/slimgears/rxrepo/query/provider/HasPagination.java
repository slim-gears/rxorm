package com.slimgears.rxrepo.query.provider;

import javax.annotation.Nullable;

public interface HasPagination extends HasLimit {
    @Nullable Long skip();
}
