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

import com.openlattice.edm.type.AssociationType
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import org.apache.commons.lang.RandomStringUtils
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.lang.reflect.UndeclaredThrowableException
import java.util.*
import kotlin.collections.LinkedHashSet


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class EdmTest : MultipleAuthenticatedUsersBase() {
    companion object {
        @JvmStatic
        @BeforeClass
        fun init() {
            loginAs("admin")
        }
    }

    private val numberOfEntries = 10

    @Test
    fun testAddAndRemoveLinkedEntitySets() {
        val pt = createPropertyType()
        val et = createEntityType(pt.id)
        val linkingEs = createEntitySet(et, true, setOf())

        val es = createEntitySet(EdmTestConstants.personEt)

        entitySetsApi.addEntitySetsToLinkingEntitySet(linkingEs.id, setOf<UUID>(es.id))
        Assert.assertEquals(es.id, edmApi.getEntitySet(linkingEs.id).linkedEntitySets.single())

        entitySetsApi.removeEntitySetsFromLinkingEntitySet(linkingEs.id, setOf(es.id))
        Assert.assertEquals(setOf<UUID>(), edmApi.getEntitySet(linkingEs.id).linkedEntitySets)
    }

    @Test
    fun testChecksOnAddAndRemoveLinkedEntitySets() {
        // entity set is not linking
        val pt = createPropertyType()
        val et = createEntityType(pt.id)
        val nonLinkingEs = createEntitySet(et, false, setOf())
        val es = createEntitySet(et)
        try {
            entitySetsApi.addEntitySetsToLinkingEntitySet(nonLinkingEs.id, setOf<UUID>(es.id))
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Can't add linked entity sets to a not linking entity set", true))
        }

        // add non-person entity set
        val linkingEs = createEntitySet(et, true, setOf())
        try {
            entitySetsApi.addEntitySetsToLinkingEntitySet(linkingEs.id, setOf<UUID>(es.id))
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains(
                            "Linked entity sets are of differing entity types than " +
                                    EdmTestConstants.personEt.type.fullQualifiedNameAsString,
                            true))
        }

        // remove empty
        try {
            entitySetsApi.removeEntitySetsFromLinkingEntitySet(linkingEs.id, setOf())
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Linked entity sets is empty", true))
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

    @Test
    fun testGetEntityTypeIdByFqn() {
        val validFqns = setOf("a.a", "a.a.a", "aaaa.aa", "A.A", "AAA.AAA", "A_a.a_A", "123.456", "\"\',!.: +-_")
        val entityTypeFqns = edmApi.entityTypes.map { it.type.fullQualifiedNameAsString }
        val entityTypeFqnsToIds = edmApi.entityTypes.associate { it.type.fullQualifiedNameAsString to it.id }
        validFqns.forEach {
            val expectedId = if (entityTypeFqns.contains(it)) {
                entityTypeFqnsToIds[it]
            } else {
                createEntityType(FullQualifiedName(it)).id
            }

            val actualId = edmApi.getEntityTypeId(FullQualifiedName(it))
            Assert.assertEquals(expectedId, actualId)
        }
    }

    @Test
    fun testDeleteEntitySet() {
        val es1 = createEntitySet(EdmTestConstants.personEt)
        val linkedEs = createEntitySet(EdmTestConstants.personEt, true, setOf(es1.id))

        val personGivenNamePropertyId = EdmTestConstants.personGivenNameId
        val entries = (1..numberOfEntries)
                .map { mapOf(personGivenNamePropertyId to setOf(RandomStringUtils.randomAscii(5))) }.toList()
        dataApi.createEntities(es1.id, entries)

        Assert.assertEquals(numberOfEntries.toLong(), dataApi.getEntitySetSize(es1.id))

        edmApi.deleteEntitySet(es1.id)

        try {
            edmApi.getEntitySet(es1.id)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Object [${es1.id}] is not accessible.", true))
        }

        val updatedLinkedEs = edmApi.getEntitySet(linkedEs.id)
        Assert.assertTrue(updatedLinkedEs.linkedEntitySets.isEmpty())
    }

    @Test
    fun testGetEntitySetIds() {
        // test empty
        Assert.assertEquals(mapOf<String, UUID>(), edmApi.getEntitySetIds(setOf<String>()))

        // test multiple
        val es1 = createEntitySet()
        val es2 = createEntitySet()
        val es3 = createEntitySet()

        Assert.assertEquals(
                mapOf(es1.name to es1.id, es2.name to es2.id, es3.name to es3.id),
                edmApi.getEntitySetIds(setOf(es1.name, es2.name, es3.name))
        )
    }
}