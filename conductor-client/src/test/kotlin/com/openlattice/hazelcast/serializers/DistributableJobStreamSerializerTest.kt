package com.openlattice.hazelcast.serializers

import com.geekbeast.rhizome.jobs.DistributableJob
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.graph.partioning.RepartitioningJob
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.RandomUtils
import org.mockito.Mockito
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class DistributableJobStreamSerializerTest : AbstractStreamSerializerTest<DistributableJobStreamSerializer, DistributableJob<*>>() {
    override fun createSerializer(): DistributableJobStreamSerializer {
        val ss = DistributableJobStreamSerializer()
        ss.setHikariDataSource(Mockito.mock(HikariDataSource::class.java))
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