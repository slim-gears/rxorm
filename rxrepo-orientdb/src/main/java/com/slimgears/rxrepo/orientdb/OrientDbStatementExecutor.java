package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.sql.SqlStatement;
import com.slimgears.rxrepo.sql.SqlStatementExecutor;
import com.slimgears.rxrepo.util.PropertyResolver;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class OrientDbStatementExecutor implements SqlStatementExecutor {
    private final static Logger log = Logger.getLogger(OrientDbStatementExecutor.class.getName());
    private final OrientDbSessionProvider sessionProvider;

    public OrientDbStatementExecutor(OrientDbSessionProvider sessionProvider) {
        this.sessionProvider = sessionProvider;
    }

    @Override
    public Observable<PropertyResolver> executeQuery(SqlStatement statement) {
        return toObservable(
                () -> {
                    logStatement("Querying", statement);
                    return sessionProvider.get().query(statement.statement(), statement.args());
                });
    }

    @Override
    public Observable<PropertyResolver> executeCommandReturnEntries(SqlStatement statement) {
        return toObservable(
                () -> {
                    logStatement("Executing command", statement);
                    return sessionProvider.get().command(statement.statement(), statement.args());
                })
                .doOnError(e -> log.severe(e::toString));
    }

    @Override
    public Single<Integer> executeCommandReturnCount(SqlStatement statement) {
        return executeCommandReturnEntries(statement)
                .map(res -> ((Long)res.getProperty("count", Long.class)).intValue())
                .first(0);
    }

    @Override
    public Observable<Notification<PropertyResolver>> executeLiveQuery(SqlStatement statement) {
        return Observable.<OrientDbLiveQueryListener.LiveQueryNotification>create(
                emitter -> {
                    logStatement("Live querying", statement);
                    OLiveQueryMonitor monitor = sessionProvider.get().live(statement.statement(), new OrientDbLiveQueryListener(emitter), statement.args());
                    emitter.setCancellable(monitor::unSubscribe);
                })
                .map(res -> Notification.ofModified(
                        Optional.ofNullable(res.oldResult())
                                .map(or -> OResultPropertyResolver.create(res::database, or))
                                .orElse(null),
                        Optional.ofNullable(res.newResult())
                                .map(or -> OResultPropertyResolver.create(res::database, or))
                                .orElse(null)));
    }

    private Observable<PropertyResolver> toObservable(Supplier<OResultSet> resultSetSupplier) {
        return Observable.<OResult>create(
                emitter -> {
                    OResultSet resultSet = resultSetSupplier.get();
                    emitter.setCancellable(resultSet::close);
                    resultSet.stream()
                            .peek(res -> log.fine(() -> "Received: " + res))
                            .forEach(emitter::onNext);
                    emitter.onComplete();
                })
                .map(res -> OResultPropertyResolver.create(sessionProvider, res));
    }

    private void logStatement(String title, SqlStatement statement) {
        log.fine(() -> title + ": " + statement.statement() + "(params: [" + Arrays.stream(statement.args()).map(Object::toString).collect(Collectors.joining(", ")) + "]");
    }
}
