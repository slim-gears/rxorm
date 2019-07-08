package com.slimgears.rxrepo.queries;

import com.slimgears.rxrepo.query.DefaultRepository;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.Repository;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RepositoryTest {
    @Mock(answer = Answers.RETURNS_MOCKS) private QueryProvider mockQueryProvider;
    private Repository repository;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        repository = new DefaultRepository(mockQueryProvider, null);
    }

    @Test
    public void testRepositoryQuery() {
        repository.entities(TestEntity.metaClass)
                .query()
                .where(TestEntity.$.key.eq(TestKey.create("aaa"))
                        .and(TestEntity.$.number.greaterThan(5))
                        .and(TestEntity.$.refEntity.text.contains("bbb")))
                .select(TestEntity.$.refEntities)
                .retrieve();
    }
}