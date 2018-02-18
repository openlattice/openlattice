package com.openlattice.postgres.mapstores.data;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore;
import com.openlattice.data.PropertyKey;
import com.openlattice.data.PropertyMetadata;
import java.util.Collection;
import java.util.Map;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PropertyMapstoreProxy implements TestableSelfRegisteringMapStore<PropertyKey, PropertyMetadata> {

    @Override public String getMapName() {
        return null;
    }

    @Override public String getTable() {
        return null;
    }

    @Override public PropertyKey generateTestKey() {
        return null;
    }

    @Override public PropertyMetadata generateTestValue() {
        return null;
    }

    @Override public MapConfig getMapConfig() {
        return null;
    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return null;
    }

    @Override public void store( PropertyKey key, PropertyMetadata value ) {

    }

    @Override public void storeAll( Map<PropertyKey, PropertyMetadata> map ) {

    }

    @Override public void delete( PropertyKey key ) {

    }

    @Override public void deleteAll( Collection<PropertyKey> keys ) {

    }

    @Override public PropertyMetadata load( PropertyKey key ) {
        return null;
    }

    @Override public Map<PropertyKey, PropertyMetadata> loadAll( Collection<PropertyKey> keys ) {
        return null;
    }

    @Override public Iterable<PropertyKey> loadAllKeys() {
        return null;
    }
}
