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
package com.openlattice.conductor.rpc

import java.util.UUID
import java.util.concurrent.Callable
import java.util.function.Function

data class ConductorElasticsearchCall<T>(
        val userId: UUID,
        val function: Function<ConductorElasticsearchApi, T>,
        private val api: ConductorElasticsearchApi?
) : Callable<T> {
    companion object {
        @JvmStatic
        fun <T> wrap(f: Function<ConductorElasticsearchApi, T>): ConductorElasticsearchCall<T> {
            return ConductorElasticsearchCall(UUID.randomUUID(), f, null)
        }
    }

    override fun call(): T {
        return function.apply(api!!)
    }
}