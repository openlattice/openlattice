package com.openlattice.hazelcast.predicates;

import static com.openlattice.postgres.mapstores.data.DataMapstoreProxy.KEY_ENTITY_SET_ID;
import static com.openlattice.postgres.mapstores.data.DataMapstoreProxy.VERSION;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntitySetPredicates {
    public static Predicate entitySet( UUID entitySetId ) {
        return Predicates
                .and( Predicates.greaterEqual( VERSION, 0 ), Predicates.equal( KEY_ENTITY_SET_ID, entitySetId ) );
    }

    private static Predicate entitySetIncludingDeleted( UUID entitySetId ) {
        return Predicates.equal( KEY_ENTITY_SET_ID, entitySetId );
    }

}
