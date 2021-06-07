package com.openlattice.hazelcast.serializers.decorators

import com.openlattice.ids.HazelcastIdGenerationService

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface IdGenerationAware {
    fun setIdGenerationService( idGenerationService: HazelcastIdGenerationService )
}