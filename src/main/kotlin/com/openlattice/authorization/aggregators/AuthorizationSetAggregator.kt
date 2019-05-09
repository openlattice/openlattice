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
package com.openlattice.authorization.aggregators

import com.hazelcast.aggregation.Aggregator
import com.openlattice.authorization.AceKey
import com.openlattice.authorization.AceValue
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Permission
import org.slf4j.LoggerFactory
import java.util.*
import java.util.Map

data class AuthorizationSetAggregator(
        val permissionsMap: MutableMap<AclKey, EnumSet<Permission>>
) : Aggregator<Map.Entry<AceKey, AceValue>, EnumSet<Permission>>() {

    companion object {
        private val logger = LoggerFactory.getLogger(AuthorizationSetAggregator::class.java)
    }

    override fun accumulate(input: Map.Entry<AceKey, AceValue>) {
        val permissions = input.value.permissions
        
        if (permissions == null) {
            logger.error("Encountered null permissions for ${input.key}")
        } else {
            // accumulate all permissions of different principals for 1 acl
            permissionsMap.getValue(input.key.aclKey).addAll(permissions)
        }
    }

    override fun combine(aggregator: Aggregator<*, *>?) {
        if (aggregator is AuthorizationSetAggregator) {
            aggregator.permissionsMap.forEach {
                permissionsMap.getValue(it.key).addAll(it.value)
            }
        }
    }

    override fun aggregate(): EnumSet<Permission> {
        if (permissionsMap.isEmpty()) {
            return EnumSet.noneOf(Permission::class.java)
        }

        return permissionsMap.values
                .fold(EnumSet.allOf(Permission::class.java)) { acc, permissionSet ->
                    val reducedPermissions = acc.intersect(permissionSet)
                    if (reducedPermissions.isEmpty()) {
                        return@fold EnumSet.noneOf(Permission::class.java)
                    }

                    EnumSet.copyOf(reducedPermissions)
                }
    }
}