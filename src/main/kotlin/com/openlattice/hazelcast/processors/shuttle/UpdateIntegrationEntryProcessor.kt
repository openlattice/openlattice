package com.openlattice.hazelcast.processors.shuttle
import com.dataloom.mappers.ObjectMappers
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.shuttle.Flight
import com.openlattice.shuttle.Integration
import com.openlattice.shuttle.IntegrationUpdate
import java.net.URL

class UpdateIntegrationEntryProcessor(val update: IntegrationUpdate) :
        AbstractRhizomeEntryProcessor<String, Integration, Integration>() {

    companion object {
        private val mapper = ObjectMappers.getYamlMapper()
    }

    override fun process(entry: MutableMap.MutableEntry<String, Integration>): Integration {
        val integration = entry.value

        update.environment.ifPresent { integration.environment = it }

        update.s3bucket.ifPresent { integration.s3bucket = it }

        update.contacts.ifPresent { integration.contacts = it }

        if (update.maxConnections.isPresent) { integration.maxConnections = update.maxConnections }

        if (update.callbackUrls.isPresent) { integration.callbackUrls = update.callbackUrls }

        update.flightPlanParameters.ifPresent {
            val flightPlanParameters = integration.flightPlanParameters
            it.forEach { entry ->
                val currentFlightPlanParameter = flightPlanParameters.getValue(entry.key)
                val update = entry.value
                if (update.sql.isPresent) { currentFlightPlanParameter.sql = update.sql.get() }
                if (update.source.isPresent) { currentFlightPlanParameter.source = update.source.get() }
                if (update.sourcePrimaryKeyColumns.isPresent) { currentFlightPlanParameter.sourcePrimaryKeyColumns = update.sourcePrimaryKeyColumns.get() }
                if (update.flightFilePath.isPresent) {
                    val updatedFlightFilePath = update.flightFilePath.get()
                    currentFlightPlanParameter.flightFilePath = updatedFlightFilePath
                    currentFlightPlanParameter.flight = mapper.readValue(URL(updatedFlightFilePath), Flight::class.java)
                }
            }

        }

        entry.setValue(integration)
        return integration
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        return true
    }

    override fun hashCode(): Int {
        return javaClass.hashCode()
    }
}