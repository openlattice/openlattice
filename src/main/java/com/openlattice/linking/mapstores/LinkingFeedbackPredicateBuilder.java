package com.openlattice.linking.mapstores;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;

import java.util.Set;
import java.util.UUID;

/**
 * Utility class for creating Predicates to use in LinkingFeedbackMapstore because of an annoying type mismatch between
 * java and kotlin with arrays
 */
public class LinkingFeedbackPredicateBuilder {
    public static Predicate inUuidArray( Set<UUID> values, String attribute) {
        return Predicates.in(attribute, values.toArray( new UUID[0] ));
    }

}
