package com.openlattice.ioc.providers

import com.openlattice.data.DataGraphService
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.DataSourceResolver
import com.openlattice.ids.HazelcastIdGenerationService

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface LateInitProvider {
    val resolver: DataSourceResolver
    val idService: HazelcastIdGenerationService
    val byteBlobDataManager: ByteBlobDataManager
    val dataGraphService: DataGraphService
}