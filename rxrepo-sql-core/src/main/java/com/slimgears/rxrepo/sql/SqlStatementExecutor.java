package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.util.PropertyResolver;
import io.reactivex.Observable;
import io.reactivex.Single;

public interface SqlStatementExecutor {
    Observable<PropertyResolver> executeQuery(SqlStatement statement);
    Observable<PropertyResolver> executeCommandReturnEntries(SqlStatement statement);
    Single<Integer> executeCommandReturnCount(SqlStatement statement);
    Observable<Notification<PropertyResolver>> executeLiveQuery(SqlStatement statement);
}
