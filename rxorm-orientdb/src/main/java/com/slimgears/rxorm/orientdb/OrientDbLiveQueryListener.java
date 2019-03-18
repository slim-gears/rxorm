package com.slimgears.rxorm.orientdb;

import com.google.auto.value.AutoValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import io.reactivex.ObservableEmitter;

import javax.annotation.Nullable;

public class OrientDbLiveQueryListener implements OLiveQueryResultListener {
    private final ObservableEmitter<LiveQueryNotification> emitter;

    public OrientDbLiveQueryListener(ObservableEmitter<LiveQueryNotification> emitter) {
        this.emitter = emitter;
    }

    @AutoValue
    public static abstract class LiveQueryNotification {
        public abstract ODatabaseDocument database();
        @Nullable public abstract OResult oldResult();
        @Nullable public abstract OResult newResult();

        public static LiveQueryNotification create(ODatabaseDocument db, OResult oldRes, OResult newRes) {
            return new AutoValue_OrientDbLiveQueryListener_LiveQueryNotification(db, oldRes, newRes);
        }
    }

    @Override
    public void onCreate(ODatabaseDocument database, OResult data) {
        emitter.onNext(LiveQueryNotification.create(database, null, data));
    }

    @Override
    public void onUpdate(ODatabaseDocument database, OResult before, OResult after) {
        emitter.onNext(LiveQueryNotification.create(database, before, after));
    }

    @Override
    public void onDelete(ODatabaseDocument database, OResult data) {
        emitter.onNext(LiveQueryNotification.create(database, data, null));
    }

    @Override
    public void onError(ODatabaseDocument database, OException exception) {
        emitter.onError(exception);
    }

    @Override
    public void onEnd(ODatabaseDocument database) {
        emitter.onComplete();
    }
}
