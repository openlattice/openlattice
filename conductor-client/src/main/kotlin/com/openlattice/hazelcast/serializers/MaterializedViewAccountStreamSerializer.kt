/*
 * Copyright (C) 2020. OpenLattice, Inc.
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

import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.directory.MaterializedViewAccount
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.apache.commons.lang3.RandomStringUtils
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class MaterializedViewAccountStreamSerializer : TestableSelfRegisteringStreamSerializer<MaterializedViewAccount> {
    override fun getTypeId(): Int = StreamSerializerTypeIds.MATERIALIZED_VIEW_ACCOUNT.ordinal

    override fun getClazz(): Class<out MaterializedViewAccount> = MaterializedViewAccount::class.java
    override fun write(out: ObjectDataOutput, obj: MaterializedViewAccount) {
        out.writeUTF(obj.username)
        out.writeUTF(obj.credential)
    }

    override fun read(input: ObjectDataInput): MaterializedViewAccount {
        val username = input.readString()!!
        val credential = input.readString()!!
        return MaterializedViewAccount(username, credential)
    }

    override fun generateTestValue(): MaterializedViewAccount = MaterializedViewAccount(
            RandomStringUtils.random(5),
            RandomStringUtils.random(5)
    )
}