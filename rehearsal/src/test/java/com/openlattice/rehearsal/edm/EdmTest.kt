/*
 * Copyright (C) 2018. OpenLattice, Inc.
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

package com.openlattice.rehearsal.edm

import com.openlattice.client.RetrofitFactory
import com.openlattice.edm.EdmApi
import com.openlattice.neuron.audit.AuditEntitySetUtils
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class EdmTest : MultipleAuthenticatedUsersBase() {
    companion object {
        init {
            val client = RetrofitFactory.newClient(RetrofitFactory.Environment.PRODUCTION, { "" })
            val prodEdmApi = client.create(EdmApi::class.java)
            val prodEdm = prodEdmApi.entityDataModel
            val entityTypes = prodEdm.entityTypes.filter { !it.type.equals(AuditEntitySetUtils.AUDIT_ET_FQN) }.toSet()
            edmApi.
                    edmApi.entityDataModel
        }
    }


}