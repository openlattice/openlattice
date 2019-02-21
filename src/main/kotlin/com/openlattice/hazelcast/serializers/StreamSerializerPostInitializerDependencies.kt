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

package com.openlattice.hazelcast.serializers

import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.HazelcastTaskDependencies
import com.openlattice.tasks.Task
import org.springframework.stereotype.Component
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class StreamSerializerPostInitializerDependencies :HazelcastTaskDependencies {
    @Inject
    private lateinit var accessConnectionManager: AssemblerConnectionManager

    @Inject
    private  lateinit var acmDependentStreamSerializers: Set<AssemblerConnectionManagerDependent>

    @Component
    class StreamSerializerPostInitializerTask: HazelcastInitializationTask<StreamSerializerPostInitializerDependencies> {
        override fun getInitialDelay(): Long {
            return 0
        }

        override fun initialize(dependencies: StreamSerializerPostInitializerDependencies) {
            dependencies.acmDependentStreamSerializers.forEach { it.init(dependencies.accessConnectionManager) }
        }

        override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
            return setOf()
        }

        override fun getName(): String {
            return Task.STREAM_SERIALIZER_POST_INITIALIZER.name
        }

        override fun getDependenciesClass(): Class<out StreamSerializerPostInitializerDependencies> {
            return StreamSerializerPostInitializerDependencies::class.java
        }
    }
}
