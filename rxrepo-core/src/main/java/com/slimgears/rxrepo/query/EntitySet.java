package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.filters.Filter;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.util.Arrays;
import java.util.List;

public interface EntitySet<K, S extends HasMetaClassWithKey<K, S>> {
    MetaClassWithKey<K, S> metaClass();
    EntityDeleteQuery<K, S> delete();
    EntityUpdateQuery<K, S> update();
    SelectQueryBuilder<K, S> query();
    Single<S> update(S entity);

    default Single<List<S>> update(Iterable<S> entities) {
        return Observable.fromIterable(entities)
                .flatMapSingle(this::update)
                .toList();
    }

    default Observable<S> update(Observable<S> entities) {
        return entities.flatMapSingle(this::update);
    }

    default Observable<S> findAll(BooleanExpression<S> predicate) {
        return query().where(predicate).select().retrieve();
    }

    default Observable<S> findAll(Filter<S> filter) {
        return findAll(filter.toExpression(ObjectExpression.arg(metaClass().objectClass())).orElse(null));
    }

    default Maybe<S> find(K key) {
        return findFirst(PropertyExpression.ofObject(metaClass().keyProperty()).eq(key));
    }

    default Maybe<S> findFirst(BooleanExpression<S> predicate) {
        return query().where(predicate).limit(1).select().first();
    }

    default Single<S[]> udpate(S[] entities) {
        return update(Arrays.asList(entities))
                .map(l -> l.toArray(entities.clone()));
    }

    default Maybe<S> update(K key, Function<Maybe<S>, Maybe<S>> updater) {
        try {
            return updater
                    .apply(find(key))
                    .flatMap(entity -> update(entity).toMaybe());
        } catch (Exception e) {
            return Maybe.error(e);
        }
    }

    default Completable clear() {
        return deleteAll(null);
    }

    default Completable delete(K key) {
        return delete()
                .where(PropertyExpression.ofObject(metaClass().keyProperty()).eq(key))
                .execute()
                .ignoreElement();
    }

    default Completable delete(K[] keys) {
        return deleteAll(PropertyExpression.ofObject(metaClass().keyProperty()).in(keys));
    }

    default Completable deleteAll(BooleanExpression<S> predicate) {
        return delete()
                .where(predicate)
                .execute()
                .ignoreElement();
    }

    default Observable<Notification<S>> observe() {
        return query().liveSelect().observe();
    }
}
