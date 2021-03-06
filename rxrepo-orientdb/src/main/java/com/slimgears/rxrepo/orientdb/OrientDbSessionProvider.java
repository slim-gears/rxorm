package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.slimgears.nanometer.MetricCollector;
import com.slimgears.nanometer.Metrics;
import com.slimgears.util.generic.RecurrentThreadLocal;
import com.slimgears.util.stream.Safe;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

class OrientDbSessionProvider implements AutoCloseable {
    private final static MetricCollector metrics = Metrics.collector(OrientDbRepository.class);
    private final static Logger log = LoggerFactory.getLogger(OrientDbSessionProvider.class);
    private final AtomicInteger currentlyActiveSessions = new AtomicInteger();
    private final MetricCollector.Gauge activeSessionsGauge = metrics.gauge("activeSessions");
    private final ExecutorService executorService;
    private final Single<ODatabaseDocument> currentSession;
    private final Single<ODatabaseDocument> session;
    private final RecurrentThreadLocal<ODatabaseDocument> sessionThreadLocal;
    private final AtomicBoolean closed = new AtomicBoolean();

    private OrientDbSessionProvider(Callable<ODatabaseDocument> databaseSessionProvider, int maxConnections) {
        Supplier<ODatabaseDocument> safeSessionProvider = Safe.ofCallable(() -> {
            ODatabaseDocument session = databaseSessionProvider.call();
            session.activateOnCurrentThread();
            return session;
        });

        sessionThreadLocal = RecurrentThreadLocal
                .of(safeSessionProvider)
                .onRelease(Safe.ofConsumer(ODatabaseDocument::close));

        this.currentSession = Single.<ODatabaseDocument>create(emitter -> {
            ODatabaseDocument s = sessionThreadLocal.acquire();
            try {
                emitter.onSuccess(s);
            } finally {
                sessionThreadLocal.release();
            }
        }).doOnSubscribe(d -> {
            int newCount = currentlyActiveSessions.incrementAndGet();
            log.trace("Created database connection (currently active connections: {})", newCount);
            activeSessionsGauge.record(newCount);
        }).doFinally(() -> {
            int newCount = currentlyActiveSessions.decrementAndGet();
            log.trace("Database connection closed (currently active connections: {}", newCount);
            activeSessionsGauge.record(newCount);
        });

        this.executorService = Executors.newFixedThreadPool(maxConnections);
        this.session = currentSession.subscribeOn(Schedulers.from(executorService));
    }

    static OrientDbSessionProvider create(Callable<ODatabaseDocument> dbSessionSupplier, int maxConnections) {
        return new OrientDbSessionProvider(dbSessionSupplier, maxConnections);
    }

    Single<ODatabaseDocument> session() {
        return session;
    }

    Single<ODatabaseDocument> currentSession() {
        return currentSession;
    }

    synchronized void withSession(Consumer<ODatabaseDocument> action) {
        if (!closed.get()) {
            this.getWithSession(session -> {
                action.accept(session);
                return null;
            });
        }
    }

    <T> T getWithSession(Function<ODatabaseDocument, T> func) {
        ODatabaseDocument session = sessionThreadLocal.acquire();
        try {
            return func.apply(session);
        } finally {
            sessionThreadLocal.release();
        }
    }

    Completable completeWithSession(io.reactivex.functions.Consumer<ODatabaseDocument> action) {
        return session().flatMapCompletable(session -> Completable.fromAction(() -> action.accept(session)));
    }

    @Override
    public void close() throws InterruptedException {
        if (closed.compareAndSet(false, true)) {
            this.executorService.shutdownNow();
            //noinspection ResultOfMethodCallIgnored
            this.executorService.awaitTermination(5, TimeUnit.SECONDS);
            log.debug("Active threads: {}", Thread.activeCount());
        }
    }
}
