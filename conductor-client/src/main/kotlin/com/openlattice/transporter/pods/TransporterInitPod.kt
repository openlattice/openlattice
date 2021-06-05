package com.openlattice.transporter.pods

import com.openlattice.transporter.tasks.TransporterInitializeServiceTask
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * @author Drew Bailey (drew@openlattice.com)
 */

@Configuration
class TransporterInitPod {

    @Bean
    fun transporterInitializeServiceTask(): TransporterInitializeServiceTask {
        LoggerFactory.getLogger(TransporterPod::class.java).info("Constructing TransporterInitializeServiceTask")
        return TransporterInitializeServiceTask()
    }
}