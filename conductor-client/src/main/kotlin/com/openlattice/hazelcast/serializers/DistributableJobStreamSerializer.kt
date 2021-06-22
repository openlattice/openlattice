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
import com.hazelcast.nio.ObjectDataInput
import com.openlattice.data.DataGraphService
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.DataSourceResolver
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.decorators.ByteBlobDataManagerAware
import com.openlattice.hazelcast.serializers.decorators.DataGraphAware
import com.openlattice.hazelcast.serializers.decorators.IdGenerationAware
import com.openlattice.hazelcast.serializers.decorators.MetastoreAware
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.ids.IdGenerationServiceDependent
import com.openlattice.ioc.providers.LateInitAware
import com.openlattice.ioc.providers.LateInitProvider
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.springframework.stereotype.Component
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@SuppressFBWarnings(value= ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"], justification="Kotlin class parsing bug in spotbugs")
@Component
class DistributableJobStreamSerializer :
    IdGenerationServiceDependent,
    AbstractDistributableJobStreamSerializer(),
    MetastoreAware,
    DataGraphAware,
    ByteBlobDataManagerAware,
    LateInitAware {

    private lateinit var resolver: DataSourceResolver

    private lateinit var idService: HazelcastIdGenerationService

    private lateinit var byteBlobDataManager: ByteBlobDataManager

    private lateinit var dataGraphService: DataGraphService

    @Inject
    private lateinit var lateInitProvider: LateInitProvider

    override fun getTypeId(): Int = StreamSerializerTypeIds.DISTRIBUTABLE_JOB.ordinal
    override fun read(`in`: ObjectDataInput): DistributableJob<*> {
        val job = super.read(`in`)
        if (job is MetastoreAware) {
            job.setDataSourceResolver(resolver)
        }
        if (job is IdGenerationAware) {
            job.setIdGenerationService(idService)
        }
        if (job is ByteBlobDataManagerAware) {
            job.setByteBlobDataManager(byteBlobDataManager)
        }
        if( job is DataGraphAware ) {
            job.setDataGraphService(dataGraphService)
        }
        if( job is LateInitAware )  {
            job.setLateInitProvider(lateInitProvider)
        }
        return job
    }

    override fun setDataSourceResolver(resolver: DataSourceResolver) {
        this.resolver = resolver
    }

    override fun setDataGraphService ( dataGraphService: DataGraphService ) {
        this.dataGraphService = dataGraphService
    }

    override fun init(idService: HazelcastIdGenerationService) {
        this.idService = idService
    }

    override fun setByteBlobDataManager(byteBlobDataManager: ByteBlobDataManager) {
        this.byteBlobDataManager = byteBlobDataManager
    }

    override fun setLateInitProvider(lateInitProvider: LateInitProvider) {
        this.lateInitProvider = lateInitProvider
    }
}