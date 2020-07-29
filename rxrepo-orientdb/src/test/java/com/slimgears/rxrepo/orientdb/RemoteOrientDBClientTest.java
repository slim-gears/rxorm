package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.slimgears.util.test.AnnotationRulesJUnit;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.observers.BaseTestConsumer;
import io.reactivex.observers.TestObserver;
import org.junit.*;
import org.junit.rules.MethodRule;

public class RemoteOrientDBClientTest {
    @Rule
    public final MethodRule annotationRules = AnnotationRulesJUnit.rule();
    private static final String dbUrl = "embedded:db";
    private static final String dbUrlRemote = "remote:10.55.136.177/db";
//    private static final String dbUrlRemote = "remote:localhost/db";
    private final static String dbName = "testDb";
    private OrientDB dbClient;

    @Before
    public void setUp() {
        if (dbClient != null) {
            dbClient.close();
        }
        dbClient = new OrientDB(dbUrlRemote, "root", "root", OrientDBConfig.defaultConfig());

        if (!dbClient.exists(dbName)) {
            dbClient.createIfNotExists(dbName, ODatabaseType.PLOCAL);
        }
    }

    @After
    public void tearDown() {
        if (dbClient != null) {
            System.out.println("dropping db: "+dbName);
            dbClient.drop(dbName);
            dbClient.close();
            dbClient = null;
        }

    }


    //the test purpose is to run it many times in a row (using intellij options to repeat untill..)
    // and at the time the test runs restart one of the nodes and examine the connections / HA of the orientDB cluster.
    @Test @Ignore
    public void testLiveQuery() throws Exception {
        try (ODatabaseSession session = dbClient.open(dbName, "admin", "admin")) {
            OClass oClass = session.createClass("MyClass");
//            OSequence sequence = session.getMetadata().getSequenceLibrary().createSequence("sequence", OSequence.SEQUENCE_TYPE.ORDERED, new OSequence.CreateParams());
            oClass.createProperty("num", OType.LONG);
            Observable<OElement> elements = Observable.<OElement>create(emitter -> {
                OLiveQueryMonitor monitor = session.live("select from MyClass", new OLiveQueryResultListener() {
                    @Override
                    public void onCreate(ODatabaseDocument database, OResult data) {
                        emitter.onNext(data.toElement());
                    }

                    @Override
                    public void onUpdate(ODatabaseDocument database, OResult before, OResult after) {
                        emitter.onNext(after.toElement());
                    }

                    @Override
                    public void onDelete(ODatabaseDocument database, OResult data) {

                    }

                    @Override
                    public void onError(ODatabaseDocument database, OException exception) {
                        exception.printStackTrace();
                        emitter.onError(exception);
                    }

                    @Override
                    public void onEnd(ODatabaseDocument database) {
                        emitter.onComplete();
                    }
                });
                emitter.setCancellable(monitor::unSubscribe);
            })
                    .retry(10, e -> e instanceof ODatabaseException/* && e.getCause() instanceof OIOException*/);
            TestObserver<OElement> testObserver = elements
                    .doOnNext((x)-> System.out.println("live query emitted value: " +x))
                    .doOnError((x)-> System.out.println("got error in live query: "+x))
                    .test();
            session.begin();
            for (int i = 1;i<1002;i++) {
                try {
                    OElement element = session.newElement("MyClass");
                    element.setProperty("num", i);
                    element.save();
                    session.commit();
                    Thread.sleep(1000);
                    System.out.println("saved element");
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    System.out.println(e.getCause());
                }
            }

            testObserver.awaitCount(300, BaseTestConsumer.TestWaitStrategy.SLEEP_10MS, 1000*60*10)
                    .assertValueCount(300)
                    .assertValueAt(0, e -> e.getProperty("num").equals(1L));

            Completable.fromAction(testObserver::dispose)
                    .blockingAwait();
        }
    }

}
