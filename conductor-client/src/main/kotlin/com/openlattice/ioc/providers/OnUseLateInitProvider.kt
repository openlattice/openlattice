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
class OnUseLateInitProvider : LateInitProvider, MetastoreAware, DataGraphAware, IdGenerationServiceDependent {
    private lateinit var _resolver: DataSourceResolver
    private lateinit var _idService: HazelcastIdGenerationService
    private lateinit var _byteBlobDataManager: ByteBlobDataManager
    private lateinit var _dataGraphService: DataGraphService

    override val resolver: DataSourceResolver
        get() = _resolver
    override val idService: HazelcastIdGenerationService
        get() = _idService
    override val byteBlobDataManager: ByteBlobDataManager
        get() = _byteBlobDataManager
    override val dataGraphService: DataGraphService
        get() = _dataGraphService

    override fun init(idService: HazelcastIdGenerationService) {
        _idService = idService
    }

    override fun setDataGraphService(dataGraphService: DataGraphService) {
        _dataGraphService = dataGraphService
    }

    override fun setDataSourceResolver(resolver: DataSourceResolver) {
        _resolver = resolver
    }

}