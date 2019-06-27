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

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organizations.SecurablePrincipalList
import org.springframework.stereotype.Component

@Component
class SecurablePrincipalListStreamSerializer
    : ListStreamSerializer<SecurablePrincipalList, SecurablePrincipal>(SecurablePrincipalList::class.java) {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SECURABLE_PRINCIPAL_LIST.ordinal
    }

    override fun newInstanceWithExpectedSize(size: Int): SecurablePrincipalList {
        return SecurablePrincipalList(ArrayList(size))
    }

    override fun readSingleElement(input: ObjectDataInput): SecurablePrincipal {
        return SecurablePrincipalStreamSerializer.deserialize(input)
    }

    override fun writeSingleElement(output: ObjectDataOutput, element: SecurablePrincipal) {
        SecurablePrincipalStreamSerializer.serialize(output, element)
    }
}