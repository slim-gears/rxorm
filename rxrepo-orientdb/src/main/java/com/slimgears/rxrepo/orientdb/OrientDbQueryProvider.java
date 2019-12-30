package com.slimgears.rxrepo.orientdb;

import com.google.common.base.Stopwatch;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.sql.*;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.stream.Optionals;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.slimgears.util.generic.LazyString.lazy;

public class OrientDbQueryProvider extends SqlQueryProvider {
    private final static Logger log = LoggerFactory.getLogger(OrientDbQueryProvider.class);
    private final OrientDbSessionProvider dbSessionProvider;

    OrientDbQueryProvider(SqlStatementProvider statementProvider,
                          SqlStatementExecutor statementExecutor,
                          SchemaProvider schemaProvider,
                          ReferenceResolver referenceResolver,
                          OrientDbSessionProvider dbSessionProvider) {
        super(statementProvider, statementExecutor, schemaProvider, referenceResolver);
        this.dbSessionProvider = dbSessionProvider;
    }

    static OrientDbQueryProvider create(SqlServiceFactory serviceFactory, OrientDbSessionProvider sessionProvider) {
        return new OrientDbQueryProvider(
                serviceFactory.statementProvider(),
                serviceFactory.statementExecutor(),
                serviceFactory.schemaProvider(),
                serviceFactory.referenceResolver(),
                sessionProvider);
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
        if (entities == null || Iterables.isEmpty(entities)) {
            return Completable.complete();
        }

        Table<MetaClass<?>, Object, OElement> queryCache = HashBasedTable.create();
        Stopwatch stopwatch = Stopwatch.createUnstarted();

        return schemaProvider.createOrUpdate(metaClass)
                .doOnSubscribe(d -> {
                    log.debug("Beginning creating class {}", lazy(metaClass::simpleName));
                    stopwatch.start();
                })
                .doOnComplete(() -> {
                    log.debug("Finished creating class {}", lazy(metaClass::simpleName));
                    log.debug("Time to create and update schemas: {}s", stopwatch.elapsed(TimeUnit.SECONDS));
                })
                .andThen(Single.just(entities)
                        .flatMap(iterable -> dbSessionProvider.withSession(dbSession -> {
                            Stopwatch sw = Stopwatch.createStarted();
                            List<OElement> oElements = Lists.newArrayList();
                            entities.forEach(entity -> oElements.add(toOrientDbObject(entity, queryCache, dbSession)));
                            log.debug("Conversion time: {}s", sw.elapsed(TimeUnit.SECONDS));
                            return Single.just(oElements);
                        }))
                        .flatMapCompletable(newElements -> dbSessionProvider.withSession(dbSession -> {
                                try {
                                    Stopwatch sw = Stopwatch.createStarted();
                                    dbSession.begin();
                                    newElements.forEach(ORecord::save);
                                    dbSession.commit();
                                    log.debug("Save time: {}s", sw.elapsed(TimeUnit.SECONDS));
                                } catch (OConcurrentModificationException | ORecordDuplicatedException e) {
                                    dbSession.rollback();
                                    return Completable.error(new ConcurrentModificationException(e.getMessage(), e));
                                } catch (Exception e) {
                                    dbSession.rollback();
                                    return Completable.error(e);
                                }
                                return Completable.complete();
                            })))
                .doOnComplete(() -> log.debug("Total insert time: {}s", stopwatch.elapsed(TimeUnit.SECONDS)));
    }

    private <S> OElement toOrientDbObject(S entity, OrientDbObjectConverter converter) {
        return (OElement)converter.toOrientDbObject(entity);
    }

    private <S> OElement toOrientDbObject(S entity, Table<MetaClass<?>, Object, OElement> queryCache, ODatabaseDocument dbSession) {
        return toOrientDbObject(entity, OrientDbObjectConverter.create(
                meta -> dbSession.newElement(schemaProvider.tableName(meta)),
                (converter, hasMetaClass) -> {
                    Object key = keyOf(hasMetaClass);
                    MetaClass<?> metaClass = hasMetaClass.metaClass();
                    OElement oElement = Optionals.or(
                            () -> Optional.ofNullable(queryCache.get(metaClass, key)),
                            () -> queryDocument(hasMetaClass, queryCache, dbSession),
                            () -> Optional.ofNullable(converter.toOrientDbObject(hasMetaClass)).map(OElement.class::cast))
                            .orElse(null);
                    if (oElement != null) {
                        queryCache.put(metaClass, key, oElement);
                    }
                    return oElement;
                }));
    }

    @SuppressWarnings("unchecked")
    private static <K, S> K keyOf(HasMetaClassWithKey<K, S> hasMetaClass) {
        return hasMetaClass.metaClass().keyOf((S)hasMetaClass);
    }

    private <K, S> Optional<OElement> queryDocument(HasMetaClassWithKey<K, S> entity, Table<MetaClass<?>, Object, OElement> queryCache, ODatabaseDocument dbSession) {
        MetaClassWithKey<K, S> metaClass = entity.metaClass();
        K keyValue = keyOf(entity);
        if (queryCache.contains(metaClass, keyValue)) {
            return Optional.of(queryCache.get(metaClass, keyValue));
        }
        SqlStatement statement = statementProvider.forQuery(QueryInfo.
                <K, S, S>builder()
                .metaClass(metaClass)
                .predicate(PropertyExpression.ofObject(metaClass.keyProperty()).eq(keyValue))
                .build());

        OResultSet queryResults = dbSession.query(statement.statement(), statement.args());
        Optional<OElement> existing = queryResults.elementStream().findAny();
        queryResults.close();
        if (existing.isPresent()) {
            queryCache.put(metaClass, keyValue, existing.get());
            return existing;
        }
        return Optional.empty();
    }
}
