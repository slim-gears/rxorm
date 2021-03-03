package com.slimgears.rxrepo.orientdb;

import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.sql.*;
import com.slimgears.rxrepo.util.SchedulingProvider;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;
import io.reactivex.Completable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class OrientDbQueryProvider extends DefaultSqlQueryProvider {
    private final static Logger log = LoggerFactory.getLogger(OrientDbQueryProvider.class);
    private final OrientDbSessionProvider dbSessionProvider;
    private final KeyEncoder keyEncoder;
    private final Cache<CacheKey, ORID> refCache = CacheBuilder.newBuilder()
            .initialCapacity(100000)
            .expireAfterAccess(Duration.ofMinutes(10))
            .concurrencyLevel(5)
            .build();

    static class CacheKey {
        private final MetaClassWithKey<?, ?> metaClass;
        private final Object key;

        CacheKey(MetaClassWithKey<?, ?> metaClass, Object key) {
            this.metaClass = metaClass;
            this.key = key;
        }

        public static CacheKey create(MetaClassWithKey<?, ?> metaClass, Object key) {
            return new CacheKey(metaClass, key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metaClass, key);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof CacheKey &&
                    Objects.equals(metaClass, ((CacheKey) obj).metaClass) &&
                    Objects.equals(key, ((CacheKey) obj).key);
        }
    }

    OrientDbQueryProvider(SqlStatementProvider statementProvider,
                          SqlStatementExecutor statementExecutor,
                          SchemaGenerator schemaGenerator,
                          ReferenceResolver referenceResolver,
                          SchedulingProvider schedulingProvider,
                          OrientDbSessionProvider dbSessionProvider,
                          KeyEncoder keyEncoder) {
        super(statementProvider, statementExecutor, schemaGenerator, referenceResolver, schedulingProvider);
        this.dbSessionProvider = dbSessionProvider;
        this.keyEncoder = keyEncoder;
    }

    static OrientDbQueryProvider create(SqlServiceFactory serviceFactory, OrientDbSessionProvider sessionProvider) {
        return new OrientDbQueryProvider(
                serviceFactory.statementProvider(),
                serviceFactory.statementExecutor(),
                serviceFactory.schemaProvider(),
                serviceFactory.referenceResolver(),
                serviceFactory.schedulingProvider(),
                sessionProvider,
                serviceFactory.keyEncoder());
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
        if (entities == null || Iterables.isEmpty(entities)) {
            return Completable.complete();
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        return schemaGenerator.useTable(metaClass)
                .andThen(Completable.fromAction(() -> createAndSaveElements(entities)))
                .doOnComplete(() -> log.debug("Total insert time: {}s", stopwatch.elapsed(TimeUnit.SECONDS)));
    }

    private <S> void createAndSaveElements(Iterable<S> entities) {
        AtomicLong seqNum = new AtomicLong();
        dbSessionProvider.withSession(dbSession -> {
            try {
                dbSession.declareIntent(new OIntentMassiveInsert());
//                dbSession.begin();
                OSequence sequence = dbSession.getMetadata().getSequenceLibrary().getSequence(OrientDbSchemaGenerator.sequenceName);
                seqNum.set(sequence.next());
                Collection<OElement> elements = Streams.fromIterable(entities)
                        .map(entity -> toOrientDbObject(entity, dbSession, seqNum.get()))
                        .peek(OElement::save)
                        .collect(Collectors.toList());
//                dbSession.commit();
            } catch (OConcurrentModificationException | ORecordDuplicatedException e) {
//                dbSession.rollback();
                throw new ConcurrentModificationException(e.getMessage(), e);
            } finally {
                dbSession.declareIntent(null);
            }
            //log.info("{} Written sequence num: {}", metaClass.simpleName(), seqNum.get());
        });
    }

    private <S> OElement toOrientDbObject(S entity, OrientDbObjectConverter converter) {
        return (OElement)converter.toOrientDbObject(entity);
    }

    @SuppressWarnings("unchecked")
    private <S> OElement toOrientDbObject(S entity, ODatabaseDocument dbSession, long seqNum) {
        return toOrientDbObject(entity, OrientDbObjectConverter.create(
                meta -> {
                    OElement element = dbSession.newElement(statementProvider.tableName((MetaClassWithKey<?, S>)meta));
                    element.setProperty(SqlFields.sequenceFieldName, seqNum);
                    return element;
                },
                (converter, hasMetaClass) -> {
                    Object key = keyOf(hasMetaClass);
                    MetaClassWithKey<?, ?> metaClass = hasMetaClass.metaClass();
                    CacheKey cacheKey = CacheKey.create(metaClass, key);
                    try {
                        return refCache.get(cacheKey, () -> queryDocument(hasMetaClass, dbSession));
                    } catch (ConcurrentModificationException | ExecutionException e) {
                        return refCache.getIfPresent(cacheKey);
                    }
                },
                keyEncoder));
    }

    @SuppressWarnings("unchecked")
    private static <K, S> K keyOf(HasMetaClassWithKey<K, S> hasMetaClass) {
        return hasMetaClass.metaClass().keyOf((S)hasMetaClass);
    }

    private <K, S> ORID queryDocument(HasMetaClassWithKey<K, S> entity, ODatabaseDocument dbSession) {
        MetaClassWithKey<K, S> metaClass = entity.metaClass();
        K keyValue = keyOf(entity);
        SqlStatement statement = statementProvider.forQuery(QueryInfo.
                <K, S, S>builder()
                .metaClass(metaClass)
                .predicate(PropertyExpression.ofObject(metaClass.keyProperty()).eq(keyValue))
                .build());

        statement = SqlStatement.create(
                statement.statement().replace("select ", "select @rid "),
                statement.args());

        OResultSet queryResults = dbSession.query(statement.statement(), statement.args());
        Optional<ORID> rid = queryResults.stream().map(rs -> rs.<ORID>getProperty("@rid")).findAny();
        queryResults.close();
        return rid.orElse(null);
    }
}
