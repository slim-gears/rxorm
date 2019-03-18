package com.slimgears.util.repository.query;

import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.repository.expressions.BooleanExpression;
import com.slimgears.util.repository.expressions.PropertyExpression;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public interface EntitySet<K, S extends HasMetaClassWithKey<K, S>> {
    MetaClassWithKey<K, S> metaClass();
    EntityDeleteQuery<K, S> delete();
    EntityUpdateQuery<K, S> update();
    SelectQueryBuilder<K, S> query();

    default Single<S> update(S entity) {
        return update(Collections.singleton(entity)).map(l -> l.get(0));
    }

    Single<List<S>> update(Iterable<S> entities);

    default Observable<S> update(Observable<S> entities) {
        return entities.flatMapSingle(this::update);
    }

    default Observable<S> find(BooleanExpression<S> predicate) {
        return query().where(predicate).select().retrieve();
    }

    default Maybe<S> find(K key) {
        return query()
                .where(PropertyExpression.ofObject(metaClass().keyProperty()).eq(key))
                .select()
                .first();
    }

    default Maybe<S> findFirst(BooleanExpression<S> predicate) {
        return query().where(predicate).limit(1).select().first();
    }

    default Single<S[]> udpate(S[] entities) {
        return update(Arrays.asList(entities))
                .map(l -> l.toArray(entities.clone()));
    }
}
