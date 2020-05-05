package com.slimgears.rxrepo.test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.slimgears.rxrepo.annotations.EntityModel;
import com.slimgears.rxrepo.annotations.Filterable;
import com.slimgears.util.autovalue.annotations.UseJacksonAnnotator;

import java.io.Serializable;

@EntityModel
@UseJacksonAnnotator
public interface UniqueIdEntity extends Serializable {
    @Filterable @JsonProperty int id();
    @Filterable @JsonProperty int areaId();
    @Filterable @JsonProperty String type();

    static UniqueIdEntity productDescriptionId(int id) {
        return UniqueId.create(id, 0, ProductDescription.class);
    }

    static UniqueIdEntity storageId(int id) {
        return UniqueId.create(id, 0, Storage.class);
    }

    static UniqueIdEntity productId(int id) {
        return UniqueId.create(id, 0, Product.class);
    }

    static UniqueIdEntity inventoryId(int id) {
        return UniqueId.create(id, 0, Inventory.class);
    }

    static UniqueIdEntity vendorId(int id) {
        return UniqueId.create(id, 0, Vendor.class);
    }

    static UniqueIdEntity manufacturerId(int id) {
        return UniqueId.create(id, 0, Manufacturer.class);
    }

    static UniqueIdEntity create(int id, int areaId, Class<?> type) {
        return UniqueId.create(id, areaId, type.getName());
    }
}
