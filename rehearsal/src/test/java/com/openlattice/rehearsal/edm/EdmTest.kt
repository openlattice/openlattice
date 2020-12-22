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

import com.google.common.collect.ImmutableList
import com.openlattice.data.requests.EntitySetSelection
import com.openlattice.data.requests.FileType
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.edm.type.AssociationType
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.rehearsal.assertException
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import org.apache.commons.lang.RandomStringUtils
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
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
        Assert.assertEquals(es.id, entitySetsApi.getEntitySet(linkingEs.id).linkedEntitySets.single())

        entitySetsApi.removeEntitySetsFromLinkingEntitySet(linkingEs.id, setOf(es.id))
        Assert.assertEquals(setOf<UUID>(), entitySetsApi.getEntitySet(linkingEs.id).linkedEntitySets)
    }

    @Test
    fun testAssociationTypeCreationWrongCategory() {
        val et = createEntityType()
        val at = AssociationType(Optional.of(et), LinkedHashSet(), LinkedHashSet(), false)

        assertException(
                { edmApi.createAssociationType(at) },
                "You cannot create an edge type with not an AssociationType category"
        )
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

        val ess = EntitySetSelection(Optional.of(EdmTestConstants.personEt.properties))
        Assert.assertEquals(numberOfEntries, dataApi.loadSelectedEntitySetData(es1.id, ess, FileType.json).toList().size)

        entitySetsApi.deleteEntitySet(es1.id)

        assertException(
                { entitySetsApi.getEntitySet(es1.id) },
                "Object [${es1.id}] is not accessible."
        )

        val updatedLinkedEs = entitySetsApi.getEntitySet(linkedEs.id)
        Assert.assertTrue(updatedLinkedEs.linkedEntitySets.isEmpty())
    }

    @Test
    fun testGetEntitySetIds() {
        // test empty
        Assert.assertEquals(mapOf<String, UUID>(), entitySetsApi.getEntitySetIds(setOf<String>()))

        // test multiple
        val es1 = createEntitySet()
        val es2 = createEntitySet()
        val es3 = createEntitySet()

        Assert.assertEquals(
                mapOf(es1.name to es1.id, es2.name to es2.id, es3.name to es3.id),
                entitySetsApi.getEntitySetIds(setOf(es1.name, es2.name, es3.name))
        )
    }

    @Test
    fun testEdmChangesToEntityType() {
        val pt1 = createPropertyType()
        val et = createEntityType(pt1.id)
        val es = createEntitySet(et)

        val testData1 = TestDataFactory.randomStringEntityData(1, et.properties)
        val ids1 = dataApi.createEntities(es.id, ImmutableList.copyOf(testData1.values)).toSet()

        val ess1 = EntitySetSelection(Optional.empty(), Optional.of(ids1))
        Assert.assertEquals(3, dataApi.loadSelectedEntitySetData(es.id, ess1, FileType.json).first().keySet().size)
        Assert.assertEquals(2, edmApi.getEntityType(et.id).properties.size)

        val pt2 = createPropertyType()
        edmApi.addPropertyTypeToEntityType(et.id, pt2.id)
        Assert.assertEquals((et.properties + pt2.id), edmApi.getEntityType(et.id).properties)
        Assert.assertEquals(3, dataApi.loadSelectedEntitySetData(es.id, ess1, FileType.json).first().keySet().size)

        val testData2 = TestDataFactory.randomStringEntityData(1, (et.properties + pt2.id))
        val ids2 = dataApi.createEntities(es.id, ImmutableList.copyOf(testData2.values)).toSet()
        val ess2 = EntitySetSelection(Optional.empty(), Optional.of(ids1 + ids2))
        Assert.assertEquals(
                4,
                dataApi.loadSelectedEntitySetData(es.id, ess2, FileType.json).map { it.keySet() }.flatten().toSet().size
        )

        (et.properties + pt2.id).forEach {
            edmApi.forceRemovePropertyTypeFromEntityType(et.id, it)
        }
        Assert.assertEquals(0, edmApi.getEntityType(et.id).properties.size)
        val esData = dataApi.loadSelectedEntitySetData(es.id, ess2, FileType.json)
        Assert.assertEquals(1, esData.first().size())

        assertException(
                { edmApi.deleteEntityType(et.id) },
                "Unable to delete entity type because it is associated with an entity set."
        )

        entitySetsApi.deleteEntitySet(es.id)
        edmApi.deleteEntityType(et.id)

        assertException(
                { edmApi.getEntityType(et.id) },
                "Entity type of id ${et.id} does not exists."
        )
    }

    @Test
    fun testEntityTypePropertyTypeMetadata() {
        val pt1 = createPropertyType()
        val et = createEntityType(pt1.id)
        val newtitle = "New Title !";
        val update =  MetadataUpdate(
                Optional.of(newtitle),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()
        )
        edmApi.updateEntityTypePropertyMetadata( et.id , pt1.id, update )
        val metadata = edmApi.getEntityTypePropertyMetadata(et.id, pt1.id);
        Assert.assertEquals(
                newtitle,
                metadata.title
        )
        Assert.assertEquals(
                pt1.description,
                metadata.description
        )

    }
}