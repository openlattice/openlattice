package com.openlattice.hazelcast.serializers

import com.geekbeast.rhizome.jobs.DistributableJob
import com.hazelcast.core.HazelcastInstance
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.data.storage.DataSourceResolver
import com.openlattice.graph.partioning.RepartitioningJob
import com.openlattice.jdbc.DataSourceManager
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.RandomUtils
import org.mockito.Mockito
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class DistributableJobStreamSerializerTest :
    AbstractStreamSerializerTest<DistributableJobStreamSerializer, DistributableJob<*>>() {
    override fun createSerializer(): DistributableJobStreamSerializer {
        val ss = DistributableJobStreamSerializer()
        val dataSourceManager = Mockito.mock(DataSourceManager::class.java)
        val hds = Mockito.mock(HikariDataSource::class.java)
        Mockito.`when`(dataSourceManager.getDefaultDataSource()).thenReturn(hds)
        val dsr = DataSourceResolver(Mockito.mock(HazelcastInstance::class.java), dataSourceManager)
        ss.setDataSourceResolver(dsr)
        return ss
    }

    override fun createInput(): DistributableJob<*> = RepartitioningJob(
        UUID.randomUUID(),
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