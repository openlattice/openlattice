package com.openlattice.edm.events;

import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.type.PropertyType;

public class PropertyTypeMetaDataUpdatedEvent {

    private PropertyType propertyType;
    private MetadataUpdate update;

    public PropertyTypeMetaDataUpdatedEvent(PropertyType propertyType, MetadataUpdate update) {
        this.propertyType = propertyType;
        this.update = update;
    }

    public PropertyType getPropertyType() { return this.propertyType; }
    public MetadataUpdate getUpdate() { return this.update; }
}
