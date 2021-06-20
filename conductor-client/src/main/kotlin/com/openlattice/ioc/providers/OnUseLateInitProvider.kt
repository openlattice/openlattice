package com.openlattice.ioc.providers

import com.openlattice.data.DataGraphService
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.DataSourceResolver
import com.openlattice.hazelcast.serializers.decorators.DataGraphAware
import com.openlattice.hazelcast.serializers.decorators.MetastoreAware
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.ids.IdGenerationServiceDependent
import java.util.concurrent.CountDownLatch

private const val RESOLVER = "RESOLVER"
private const val ID_SERVICE = "idService"
private const val BYTE_BLOB_DATA_MANAGER = "bBDM"
private const val DATA_GRAPH_SERVICE = "dGS"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class OnUseLateInitProvider : LateInitProvider, MetastoreAware, DataGraphAware, IdGenerationServiceDependent {
    private lateinit var _resolver: DataSourceResolver
    private lateinit var _idService: HazelcastIdGenerationService
    private lateinit var _byteBlobDataManager: ByteBlobDataManager
    private lateinit var _dataGraphService: DataGraphService

    private val latches = mutableMapOf<String, CountDownLatch>()

    override val resolver: DataSourceResolver
        get() = latchedGet(RESOLVER, _resolver)
    override val idService: HazelcastIdGenerationService
        get() = latchedGet(ID_SERVICE, _idService)
    override val byteBlobDataManager: ByteBlobDataManager
        get() = latchedGet(BYTE_BLOB_DATA_MANAGER, _byteBlobDataManager)
    override val dataGraphService: DataGraphService
        get() = latchedGet(DATA_GRAPH_SERVICE, _dataGraphService)

    override fun init(idService: HazelcastIdGenerationService) {
        _idService = idService
        countdown(ID_SERVICE)
    }

    override fun setDataGraphService(dataGraphService: DataGraphService) {
        _dataGraphService = dataGraphService
        countdown(DATA_GRAPH_SERVICE)
    }

    override fun setDataSourceResolver(resolver: DataSourceResolver) {
        _resolver = resolver
        countdown(RESOLVER)
    }

    private fun countdown(key: String) {
        latches.getOrPut(key) { CountDownLatch(1) }.countDown()
    }

    private fun <T> latchedGet(key: String, retval: T): T {
        latches.getOrPut(key) { CountDownLatch(1) }.await()
        return retval
    }
}