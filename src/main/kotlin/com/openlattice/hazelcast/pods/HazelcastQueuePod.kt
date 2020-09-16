/*
 * Copyright (C) 2019. OpenLattice, Inc.
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
package com.openlattice.hazelcast.pods

import com.kryptnostic.rhizome.pods.hazelcast.BaseHazelcastInstanceConfigurationPod
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.ids.HazelcastIdGenerationService
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class HazelcastQueuePod {

    companion object {
        @JvmField
        val maxQueueSize = 10_000
    }

    @Bean
    fun defaultQueueConfigurer(): QueueConfigurer {
        return QueueConfigurer(BaseHazelcastInstanceConfigurationPod.defaultQueueName) { config ->
            config.maxSize = maxQueueSize
        }
    }

    @Bean
    fun idGenerationQueueConfigurer(): QueueConfigurer {
        return QueueConfigurer(HazelcastQueue.ID_GENERATION.name) { config ->
            config.setMaxSize(HazelcastIdGenerationService.NUM_PARTITIONS * 10).backupCount = 1
        }
    }

    @Bean
    fun twilioQueueConfigurer(): QueueConfigurer {
        return QueueConfigurer(HazelcastQueue.TWILIO.name) { config ->
            config.setMaxSize(10_0000).backupCount = 1
        }
    }

    @Bean
    fun indexingQueueConfigurer(): QueueConfigurer {
        return QueueConfigurer(HazelcastQueue.INDEXING.name) { config ->
            config.setMaxSize(10_0000).backupCount = 1
        }
    }

    @Bean
    fun linkingQueueConfigurer(): QueueConfigurer {
        return QueueConfigurer(HazelcastQueue.LINKING_CANDIDATES.name) { config ->
            config.setMaxSize(1_000).backupCount = 1
        }
    }

    @Bean
    fun linkingIndexingQueueConfigurer(): QueueConfigurer {
        return QueueConfigurer(HazelcastQueue.LINKING_INDEXING.name) { config ->
            config.setMaxSize(10_000).backupCount = 1
        }
    }

    @Bean
    fun linkingUnIndexingQueueConfigurer(): QueueConfigurer {
        return QueueConfigurer(HazelcastQueue.LINKING_UNINDEXING.name) { config ->
            config.setMaxSize(10_000).backupCount = 1
        }
    }

    @Bean
    fun integrationJobQueueConfigurer(): QueueConfigurer {
        return QueueConfigurer(HazelcastQueue.QUEUED_INTEGRATION_JOBS.name) { config ->
            config.setMaxSize(1_000).backupCount = 1
        }
    }

}