/*
 * Copyright (C) 2020. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.hazelcast.serializers

import com.geekbeast.rhizome.jobs.AbstractDistributableJobStreamSerializer
import com.geekbeast.rhizome.jobs.DistributableJob
import com.google.common.annotations.VisibleForTesting
import com.hazelcast.nio.ObjectDataInput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.decorators.IdGenerationAware
import com.openlattice.hazelcast.serializers.decorators.MetastoreAware
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.ids.IdGenerationServiceDependent
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class DistributableJobStreamSerializer : IdGenerationServiceDependent<DistributableJobStreamSerializer>, AbstractDistributableJobStreamSerializer(){
    @Inject
    private lateinit var hds: HikariDataSource

    private lateinit var idService: HazelcastIdGenerationService

    override fun getTypeId(): Int = StreamSerializerTypeIds.DISTRIBUTABLE_JOB.ordinal
    override fun read(`in`: ObjectDataInput): DistributableJob<*> {
        val job = super.read(`in`)
        if (job is MetastoreAware) {
            job.setHikariDataSource(hds)
        }
        if (job is IdGenerationAware) {
            job.setIdGenerationService(idService)
        }
        return job
    }

    @VisibleForTesting
    internal fun setHikariDataSource(hds: HikariDataSource) {
        this.hds = hds
    }

    override fun init(idService: HazelcastIdGenerationService): DistributableJobStreamSerializer {
        this.idService = idService
        return this
    }
}