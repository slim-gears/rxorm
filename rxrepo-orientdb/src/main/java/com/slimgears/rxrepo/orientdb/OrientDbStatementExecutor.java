package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.slimgears.util.generic.LazyToString.lazy;

class OrientDbStatementExecutor implements SqlStatementExecutor {
    private final static Logger log = LoggerFactory.getLogger(OrientDbStatementExecutor.class);
    private final static long timeoutMillis = 10000;
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
                    try {
                        logStatement("Executing command", statement);
                        return session.command(statement.statement(), convertArgs(statement.args()));
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
                .subscribeOn(scheduler)
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
                            .peek(res -> log.trace("Received: {}", res))
                            .forEach(emitter::onNext);
                    emitter.onComplete();
                }))
                .map(res -> OResultPropertyResolver.create(sessionProvider, res))
                .subscribeOn(scheduler)
                .timeout(timeoutMillis, TimeUnit.MILLISECONDS);
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
        if (obj instanceof Collection) {
            return convertCollection((Collection<?>)obj);
        } else if (obj instanceof Map) {
            return convertMap((Map<?, ?>)obj);
        } else if (!(obj instanceof HasMetaClass)) {
            return obj;
        }

        HasMetaClass<?> hasMetaClass = (HasMetaClass)obj;
        MetaClass<?> metaClass = hasMetaClass.metaClass();
        OElement oElement = new ODocument(OrientDbSchemaProvider.toClassName(metaClass));
        metaClass.properties().forEach(p -> oElement.setProperty(p.name(), convertArg(((PropertyMeta)p).getValue(obj))));

        return oElement;
    }

    private Map<?, ?> convertMap(Map<?, ?> map) {
        return map.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> convertArg(e.getKey()), e -> convertArg(e.getValue())));
    }

    private Collection<?> convertCollection(Collection<?> collection) {
        if (collection instanceof List) {
            return collection.stream().map(this::convertArg).collect(Collectors.toList());
        } else if (collection instanceof Set) {
            return collection.stream().map(this::convertArg).collect(Collectors.toSet());
        }
        return collection;
    }

    private void logStatement(String title, SqlStatement statement) {
        log.trace("{}: {}", title, lazy(() -> toString(statement)));
    }

    private String toString(SqlStatement statement) {
        return statement.statement() + "(params: [" + Arrays.stream(statement.args()).map(Object::toString).collect(Collectors.joining(", ")) + "]";
    }
}
