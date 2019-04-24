package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.sql.SqlStatement;
import com.slimgears.rxrepo.sql.SqlStatementExecutor;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.Single;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

class OrientDbStatementExecutor implements SqlStatementExecutor {
    private final static Logger log = Logger.getLogger(OrientDbStatementExecutor.class.getName());
    private final OrientDbSessionProvider sessionProvider;
    private final Scheduler scheduler;
    private final Completable shutdown;

    OrientDbStatementExecutor(OrientDbSessionProvider sessionProvider, Scheduler scheduler, Completable shutdown) {
        this.shutdown = shutdown;
        this.sessionProvider = sessionProvider;
        this.scheduler = scheduler;
    }

    @Override
    public Observable<PropertyResolver> executeQuery(SqlStatement statement) {
        return toObservable(
                session -> {
                    logStatement("Querying", statement);
                    return session.query(statement.statement(), convertArgs(statement.args()));
                });
    }

    @Override
    public Observable<PropertyResolver> executeCommandReturnEntries(SqlStatement statement) {
        return toObservable(
                session -> {
                    logStatement("Executing command", statement);
                    return session.command(statement.statement(), convertArgs(statement.args()));
                })
                .doOnError(e -> log.severe(e::toString))
                .subscribeOn(scheduler);
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
                    sessionProvider.withSession(dbSession -> {
                        OLiveQueryMonitor monitor = dbSession.live(statement.statement(), new OrientDbLiveQueryListener(emitter), convertArgs(statement.args()));
                        emitter.setCancellable(monitor::unSubscribe);
                    });
                })
                .map(res -> Notification.ofModified(
                        Optional.ofNullable(res.oldResult())
                                .map(or -> OResultPropertyResolver.create(OrientDbSessionProvider.create(res::database), or))
                                .orElse(null),
                        Optional.ofNullable(res.newResult())
                                .map(or -> OResultPropertyResolver.create(OrientDbSessionProvider.create(res::database), or))
                                .orElse(null)))
                .takeUntil(shutdown.andThen(Observable.just(0)))
                .observeOn(scheduler);
    }

    private Observable<PropertyResolver> toObservable(Function<ODatabaseDocument, OResultSet> resultSetSupplier) {
        return Observable.<OResult>create(
                emitter -> sessionProvider.withSession(dbSession -> {
                    OResultSet resultSet = resultSetSupplier.apply(dbSession);
                    emitter.setCancellable(resultSet::close);
                    resultSet.stream()
                            .peek(res -> log.fine(() -> "Received: " + res))
                            .forEach(emitter::onNext);
                    emitter.onComplete();
                }))
                .map(res -> OResultPropertyResolver.create(sessionProvider, res))
                .subscribeOn(scheduler);
    }

    private Object[] convertArgs(Object[] args) {
        Object[] newArgs = Arrays.copyOf(args, args.length);
        for (int i = 0; i < args.length; ++i) {
            newArgs[i] = convertArg(newArgs[i]);
        }
        return newArgs;
    }

    @SuppressWarnings("unchecked")
    private Object convertArg(Object obj) {
        if (!(obj instanceof HasMetaClass)) {
            return obj;
        }

        HasMetaClass<?> hasMetaClass = (HasMetaClass)obj;
        MetaClass<?> metaClass = hasMetaClass.metaClass();
        OElement oElement = new ODocument(OrientDbSchemaProvider.toClassName(metaClass.objectClass()));
        metaClass.properties().forEach(p -> oElement.setProperty(p.name(), convertArg(((PropertyMeta)p).getValue(obj))));

        return oElement;
    }

    private void logStatement(String title, SqlStatement statement) {
        log.fine(() -> title + ": " + statement.statement() + "(params: [" + Arrays.stream(statement.args()).map(Object::toString).collect(Collectors.joining(", ")) + "]");
    }
}
