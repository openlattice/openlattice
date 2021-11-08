package com.openlattice.hazelcast.serializers

import com.geekbeast.rhizome.jobs.DistributableJob
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.data.integration.Entity
import com.openlattice.data.storage.DataSourceResolver
import com.openlattice.edm.EntitySet
import com.openlattice.graph.partioning.RepartitioningJob
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.InternalTestDataFactory
import com.openlattice.jdbc.DataSourceManager
import com.openlattice.mapstores.TestDataFactory
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import org.mockito.Mockito
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class DistributableJobStreamSerializerTest :
        AbstractStreamSerializerTest<DistributableJobStreamSerializer, DistributableJob<*>>() {
    private lateinit var id:UUID
    override fun createSerializer(): DistributableJobStreamSerializer {
        val dataSourceManager: DataSourceManager = Mockito.mock(DataSourceManager::class.java)
        val m = Mockito.mock(IMap::class.java) as IMap<UUID, EntitySet>
        val hz = Mockito.mock(HazelcastInstance::class.java)
        val hds = Mockito.mock(HikariDataSource::class.java)
        val entitySet = TestDataFactory.entitySet()
        entitySet.datastore = "test"
        id = entitySet.id

        Mockito.`when`(hz.getMap<UUID, EntitySet>(HazelcastMap.ENTITY_SETS.name)).thenReturn(m)
        Mockito.`when`(dataSourceManager.getDefaultDataSource()).thenReturn(hds)
        Mockito.`when`(m.get(id)).thenReturn( entitySet )

        val dsr = DataSourceResolver(hz, dataSourceManager)
        val ss = DistributableJobStreamSerializer()
        ss.setDataSourceResolver(dsr)
        return ss
    }

    override fun createInput(): DistributableJob<*> {
        return RepartitioningJob(id,
                listOf(
                        RandomUtils.nextInt(),
                        RandomUtils.nextInt(),
                        RandomUtils.nextInt()
                ),
                setOf(
                        RandomUtils.nextInt(),
                        RandomUtils.nextInt(),
                        RandomUtils.nextInt()
                )
        )

    }
}