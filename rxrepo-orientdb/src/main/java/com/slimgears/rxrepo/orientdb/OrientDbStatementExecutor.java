package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.sql.SqlStatement;
import com.slimgears.rxrepo.sql.SqlStatementExecutor;
import com.slimgears.rxrepo.util.PropertyResolver;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.slimgears.rxrepo.orientdb.OrientDbObjectConverter.toOrientDbObjects;
import static com.slimgears.util.generic.LazyString.lazy;

class OrientDbStatementExecutor implements SqlStatementExecutor {
    private final static Logger log = LoggerFactory.getLogger(OrientDbStatementExecutor.class);
    private final static long timeoutMillis = 10000;
    private final OrientDbSessionProvider sessionProvider;
    private final Completable shutdown;

    OrientDbStatementExecutor(OrientDbSessionProvider sessionProvider, Completable shutdown) {
        this.shutdown = shutdown;
        this.sessionProvider = sessionProvider;
    }

    @Override
    public Observable<PropertyResolver> executeQuery(SqlStatement statement) {
        return toObservable(
                session -> {
                    logStatement("Querying", statement);
                    return session.query(statement.statement(), toOrientDbObjects(statement.args()));
                });
    }

    @Override
    public Observable<PropertyResolver> executeCommandReturnEntries(SqlStatement statement) {
        return toObservable(
                session -> {
                    try {
                        logStatement("Executing command", statement);
                        return session.command(statement.statement(), toOrientDbObjects(statement.args()));
                    } catch (OConcurrentModificationException | ORecordDuplicatedException e) {
                        throw new ConcurrentModificationException(e.getMessage(), e);
                    } catch (Throwable e) {
                        log.debug("Error when executing {}", lazy(() -> toString(statement)), e);
                        throw e;
                    }
                    finally {
                        logStatement("Executed command", statement);
                    }
                })
                .timeout(timeoutMillis,
                        TimeUnit.MILLISECONDS,
                        Observable.error(new TimeoutException("Timeout when executing: " + toString(statement))));
    }

    @Override
    public Single<Integer> executeCommandReturnCount(SqlStatement statement) {
        return executeCommandReturnEntries(statement)
                .map(res -> ((Long)res.getProperty("count", Long.class)).intValue())
                .first(0)
                .timeout(timeoutMillis,
                        TimeUnit.MILLISECONDS,
                        Single.error(new TimeoutException("Timeout when executing: " + toString(statement))));
    }

    @Override
    public Completable executeCommand(SqlStatement statement) {
        return toObservable(session -> session.command(statement.statement(), toOrientDbObjects(statement.args())))
                .ignoreElements();
    }

    @Override
    public Observable<Notification<PropertyResolver>> executeLiveQuery(SqlStatement statement) {
        return Observable.<OrientDbLiveQueryListener.LiveQueryNotification>create(
                emitter -> {
                    logStatement("Live querying", statement);
                    sessionProvider.withSession(dbSession -> {
                        OLiveQueryMonitor monitor = dbSession.live(
                                statement.statement(),
                                new OrientDbLiveQueryListener(emitter),
                                toOrientDbObjects(statement.args()));
                        emitter.setCancellable(monitor::unSubscribe);
                    });
                })
                .map(res -> Notification.ofModified(
                        Optional.ofNullable(res.oldResult())
                                .map(or -> OResultPropertyResolver.create(OrientDbSessionProvider.create(res.database()), or))
                                .orElse(null),
                        Optional.ofNullable(res.newResult())
                                .map(or -> OResultPropertyResolver.create(OrientDbSessionProvider.create(res.database()), or))
                                .orElse(null)))
                .takeUntil(shutdown.andThen(Observable.just(0)));
    }

    private Observable<PropertyResolver> toObservable(Function<ODatabaseDocument, OResultSet> resultSetSupplier) {
        return Observable.<OResult>create(
                emitter -> sessionProvider.withSession(dbSession -> {
                    OResultSet resultSet = resultSetSupplier.apply(dbSession);
                    emitter.setCancellable(resultSet::close);
                    resultSet.stream()
                            .peek(res -> log.trace("Received: {}", res))
                            .forEach(emitter::onNext);
                    emitter.onComplete();
                }))
                .map(res -> OResultPropertyResolver.create(sessionProvider, res))
                .timeout(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    private void logStatement(String title, SqlStatement statement) {
        log.trace("{}: {}", title, lazy(() -> toString(statement)));
    }

    private String toString(SqlStatement statement) {
        return statement.statement() + "(params: [" + Arrays.stream(statement.args()).map(Object::toString).collect(Collectors.joining(", ")) + "]";
    }
}
