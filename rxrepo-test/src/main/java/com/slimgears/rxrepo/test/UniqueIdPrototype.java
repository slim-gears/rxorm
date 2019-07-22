package com.slimgears.rxrepo.test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.slimgears.rxrepo.annotations.Filterable;
import com.slimgears.rxrepo.annotations.UseExpressions;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;
import com.slimgears.util.autovalue.annotations.UseJacksonAnnotator;

import java.io.Serializable;

@AutoValuePrototype
@UseExpressions
@UseCopyAnnotator
@UseJacksonAnnotator
public interface UniqueIdPrototype extends Serializable {
    @Filterable @JsonProperty int id();
    @Filterable @JsonProperty int areaId();
    @Filterable @JsonProperty Class<?> type();

    static UniqueIdPrototype storageId(int id) {
        return UniqueId.create(id, 0, Storage.class);
    }

    static UniqueIdPrototype productId(int id) {
        return UniqueId.create(id, 0, Product.class);
    }

    static UniqueIdPrototype inventoryId(int id) {
        return UniqueId.create(id, 0, Inventory.class);
    }

    static UniqueIdPrototype vendorId(int id) {
        return UniqueId.create(id, 0, Vendor.class);
    }
}
