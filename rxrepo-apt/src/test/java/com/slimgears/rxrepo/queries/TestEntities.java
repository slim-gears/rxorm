package com.slimgears.rxrepo.queries;

import java.util.Collections;

class TestEntities {
    static final String textEntity1 = "Entity 1";

    static final TestEntity testEntity1 = TestEntity.builder()
            .number(3)
            .text(textEntity1)
            .refEntity(TestRefEntity
                    .builder()
                    .text("Description 1")
                    .id(10)
                    .build())
            .keyName("Key 1")
            .refEntities(Collections.emptyList())
            .build();

    static final TestEntity testEntity2 = TestEntity.builder()
            .number(8)
            .text("Entity 2")
            .refEntity(TestRefEntity
                    .builder()
                    .text("Description 2")
                    .id(10)
                    .build())
            .keyName("Key 2")
            .refEntities(Collections.emptyList())
            .address("Address")
            .col(Collections.singleton("Address"))
            .build();
}
