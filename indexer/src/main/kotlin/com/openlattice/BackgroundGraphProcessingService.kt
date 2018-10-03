package com.openlattice

import com.google.common.base.Stopwatch
import com.openlattice.graph.processing.GraphProcessingService
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 * Service responsible for populating aggregated data for certain types of entities to show it in top utilizers analytics.
 * The background job runs at fixed times and updates the aggregated values if it is empty or a entity changed, that affected its value.
 */

private const val AGGREGATION_MS = 30000L

class BackgroundGraphProcessingService(private val gps:GraphProcessingService) {

    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundGraphProcessingService::class.java)
    }

    //@Scheduled(fixedRate = com.openlattice.AGGREGATION_MS)
    fun processActiveEnities() {
        logger.info("Starting graph processing background service.")
        val watch = Stopwatch.createStarted()
        gps.step()
        logger.info("Graph processing background service finished in ${watch.elapsed(TimeUnit.MILLISECONDS)} ms!")
    }
}