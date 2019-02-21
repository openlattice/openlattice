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

package com.openlattice.tasks

import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.hazelcast.serializers.AssemblerConnectionManagerDependent
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import javax.inject.Inject

private val logger = LoggerFactory.getLogger(PostConstructInitializerTaskDependencies::class.java)

/**
 * This class is a more structed equivalent ot using @PostConstruct which being deprecated.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class PostConstructInitializerTaskDependencies : HazelcastTaskDependencies {
    @Inject
    private lateinit var accessConnectionManager: AssemblerConnectionManager

    @Inject
    private lateinit var acmDependentStreamSerializers: Set<AssemblerConnectionManagerDependent>


    @Component
    class PostConstructInitializerTask : HazelcastInitializationTask<PostConstructInitializerTaskDependencies> {
        override fun getInitialDelay(): Long {
            return 0
        }

        override fun initialize(dependencies: PostConstructInitializerTaskDependencies) {
            dependencies.acmDependentStreamSerializers.forEach {
                it.init(dependencies.accessConnectionManager)
                logger.info("Initialized ${it.javaClass} with ACM")
            }
        }

        override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
            return setOf()
        }

        override fun getName(): String {
            return Task.POST_INITIALIZER.name
        }

        override fun getDependenciesClass(): Class<out PostConstructInitializerTaskDependencies> {
            return PostConstructInitializerTaskDependencies::class.java
        }

        override fun isRunOnceAcrossCluster(): Boolean {
            return false
        }
    }
}
