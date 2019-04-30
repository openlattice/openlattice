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

import com.google.common.base.Preconditions.checkNotNull
import com.google.common.collect.Sets
import com.hazelcast.aggregation.Aggregator
import com.openlattice.authorization.AceKey
import com.openlattice.authorization.AceValue
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Permission
import java.util.*
import java.util.Map

data class AuthorizationSetAggregator(
        val permissionsMap: MutableMap<AclKey, EnumSet<Permission>>
) : Aggregator<Map.Entry<AceKey, AceValue>, EnumSet<Permission>>() {

    override fun accumulate(input: Map.Entry<AceKey, AceValue>) {
        val permissions = checkNotNull(input.value.permissions, "Permissions shouldn't be null")
        permissionsMap[input.key.aclKey] = permissions
    }

    override fun combine(aggregator: Aggregator<*, *>?) {
        if (aggregator is AuthorizationSetAggregator) {
            aggregator.permissionsMap.forEach {
                permissionsMap[it.key]!!.addAll(it.value)
            }
        }
    }

    override fun aggregate(): EnumSet<Permission> {
        if (permissionsMap.isEmpty()) {
            return EnumSet.noneOf(Permission::class.java)
        }

        return permissionsMap.values
                .fold(EnumSet.allOf(Permission::class.java)) { acc, permissionSet ->
                    val reducedPermissions = Sets.intersection(acc, permissionSet)
                    if (reducedPermissions.isEmpty()) {
                        return@fold EnumSet.noneOf(Permission::class.java)
                    }

                    EnumSet.copyOf(reducedPermissions)
                }
    }

    override fun toString(): String {
        return "AuthorizationSetAggregator{permissionsMap=$permissionsMap}"
    }
}