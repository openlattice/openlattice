package com.openlattice.hazelcast.serializers.decorators

import com.openlattice.data.DataGraphService

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface DataGraphAware {
    fun setDataGraphService( dataGraphService: DataGraphService)
}