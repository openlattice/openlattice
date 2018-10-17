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

import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.util.UUID

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class EdmTest : MultipleAuthenticatedUsersBase() {
    companion object {
        @JvmStatic @BeforeClass fun init() {
            loginAs("admin")
        }
    }

    @Test
    fun testAddAndRemoveLinkedEntitySets() {
        val pt = createPropertyType()
        val et = createEntityType( pt.id )
        val linkingEs = createEntitySet( et, true, setOf() )
        val es = createEntitySet()

        linkingApi.addEntitySetsToLinkingEntitySet( linkingEs.id, setOf<UUID>(es.id) )
        Assert.assertEquals( es.id, edmApi.getEntitySet( linkingEs.id ).linkedEntitySets.single() )

        linkingApi.removeEntitySetsFromLinkingEntitySet( linkingEs.id, setOf(es.id) )
        Assert.assertEquals( setOf<UUID>(), edmApi.getEntitySet( linkingEs.id ).linkedEntitySets )
    }

    @Test
    fun testChecksOnAddAndRemoveLinkedEntitySets() {
        // TODO: finish, when error catching is merged
    }
}