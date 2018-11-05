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
import com.openlattice.edm.type.AssociationType
import com.openlattice.rehearsal.GeneralException
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.util.UUID
import org.junit.Rule
import java.util.*
import kotlin.collections.LinkedHashSet
import org.junit.rules.ExpectedException
import java.lang.reflect.UndeclaredThrowableException


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class EdmTest : MultipleAuthenticatedUsersBase() {
    private val PERSON_NAMESPACE = "general"
    private val PERSON_NAME = "person"

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

        val personEntityTypeId = edmApi.getEntityTypeId( PERSON_NAMESPACE, PERSON_NAME )
        val personEt = edmApi.getEntityType( personEntityTypeId )
        val es = createEntitySet( personEt )

        linkingApi.addEntitySetsToLinkingEntitySet( linkingEs.id, setOf<UUID>(es.id) )
        Assert.assertEquals( es.id, edmApi.getEntitySet( linkingEs.id ).linkedEntitySets.single() )

        linkingApi.removeEntitySetsFromLinkingEntitySet( linkingEs.id, setOf(es.id) )
        Assert.assertEquals( setOf<UUID>(), edmApi.getEntitySet( linkingEs.id ).linkedEntitySets )
    }

    @Test
    fun testChecksOnAddAndRemoveLinkedEntitySets() {
        // entity set is not linking
        val pt = createPropertyType()
        val et = createEntityType( pt.id )
        val nonLinkingEs = createEntitySet( et, false, setOf() )
        val es = createEntitySet( et )
        try {
            linkingApi.addEntitySetsToLinkingEntitySet( nonLinkingEs.id, setOf<UUID>(es.id) )
            Assert.fail("Should have thrown Exception but did not!")
        } catch( e: UndeclaredThrowableException ) {
            Assert.assertTrue( e.undeclaredThrowable.message!!
                    .contains( "Can't add linked entity sets to a not linking entity set", true ) )
        }

        // add non-person entity set
        val linkingEs = createEntitySet( et, true, setOf() )
        try {
            linkingApi.addEntitySetsToLinkingEntitySet( linkingEs.id, setOf<UUID>(es.id) )
            Assert.fail("Should have thrown Exception but did not!")
        } catch( e: UndeclaredThrowableException ) {
            Assert.assertTrue( e.undeclaredThrowable.message!!
                    .contains(
                            "Linked entity sets are of differing entity types than $PERSON_NAMESPACE.$PERSON_NAME",
                            true ) )
        }

        // remove empty
        try {
            linkingApi.removeEntitySetsFromLinkingEntitySet( linkingEs.id, setOf() )
            Assert.fail("Should have thrown Exception but did not!")
        } catch( e: UndeclaredThrowableException ) {
            Assert.assertTrue( e.undeclaredThrowable.message!!
                    .contains( "Linked entity sets is empty", true ) )
        }

    }

    @Test
    fun testAssociationTypeCreationWrongCategory() {
        val et = MultipleAuthenticatedUsersBase.createEntityType()
        val at = AssociationType(Optional.of(et), LinkedHashSet(), LinkedHashSet(), false)

        try {
            edmApi.createAssociationType(at)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("You cannot create an edge type with not an AssociationType category", true))
        }
    }
}