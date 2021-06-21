package com.openlattice.ioc.providers

import com.openlattice.data.DataGraphService
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.DataSourceResolver
import com.openlattice.hazelcast.serializers.decorators.DataGraphAware
import com.openlattice.hazelcast.serializers.decorators.MetastoreAware
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.ids.IdGenerationServiceDependent

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface LateInitProvider : MetastoreAware, DataGraphAware, IdGenerationServiceDependent {
    val resolver: DataSourceResolver
    val idService: HazelcastIdGenerationService
    val byteBlobDataManager: ByteBlobDataManager
    val dataGraphService: DataGraphService
}