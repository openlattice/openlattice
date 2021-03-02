package com.openlattice.hazelcast.processors.shuttle

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.shuttle.IntegrationJob
import com.openlattice.shuttle.IntegrationStatus
import org.springframework.stereotype.Component
import java.util.*

@Component
class UpdateIntegrationStatusEntryProcessor(val status: IntegrationStatus) :
        AbstractRhizomeEntryProcessor<UUID, IntegrationJob, IntegrationJob>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, IntegrationJob>): IntegrationJob {
        val job = entry.value
        job.integrationStatus = status
        entry.setValue(job)
        return job
    }
}