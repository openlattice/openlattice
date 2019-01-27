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

package com.openlattice.rehearsal.data

import com.google.common.collect.*
import com.openlattice.data.DeleteType
import com.openlattice.data.requests.EntitySetSelection
import com.openlattice.data.requests.FileType
import com.openlattice.postgres.DataTables
import com.openlattice.rehearsal.SetupTestData
import com.openlattice.rehearsal.edm.PERSON_GIVEN_NAME_NAME
import com.openlattice.rehearsal.edm.PERSON_GIVEN_NAME_NAMESPACE
import com.openlattice.rehearsal.edm.PERSON_NAME
import com.openlattice.rehearsal.edm.PERSON_NAMESPACE
import org.apache.commons.lang.RandomStringUtils
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

private val numberOfEntries = 10

class DataControllerLinkingTest : SetupTestData() {
    companion object {
        @JvmStatic
        @BeforeClass
        fun init() {
            importDataSet("socratesA.yaml", "test_linked_ppl_1.csv")
            importDataSet("associationTestFlight.yaml", "emptyTestData.csv")

            Thread.sleep(5000)
            while (!checkLinkingFinished(setOf("SocratesTestA"))) {
                Thread.sleep(3000)
            }
        }
    }

    @Test
    fun testGetLinkedEntitySets() {
        val personEntityTypeId = edmApi.getEntityTypeId(PERSON_NAMESPACE, PERSON_NAME)
        val personEt = edmApi.getEntityType(personEntityTypeId)

        // these ids are put into the whitelist configuration in indexer > linking.yaml
        // otherwise it will never get picked up by linking
        val esId1 = UUID.fromString("65847f7b-c939-414f-bb45-863cd594e412")
        val esId2 = UUID.fromString("8c97bf63-ca7a-4f37-ba1c-40338b1032d4")

        val es1 = try {
            createEntitySet(esId1, personEt)
        } catch (e: Exception) {
            edmApi.getEntitySet(esId1)
        }
        val es2 = try {
            createEntitySet(esId2, personEt)
        } catch (e: Exception) {
            edmApi.getEntitySet(esId2)
        }

        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Soft)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Hard)

        val pt = createPropertyType()
        val et = createEntityType(pt.id)
        val esLinked = createEntitySet(et, true, setOf(esId1, esId2))

        val personGivenNamePropertyId = edmApi.getPropertyTypeId(PERSON_GIVEN_NAME_NAMESPACE, PERSON_GIVEN_NAME_NAME)
        val givenNames = mapOf(personGivenNamePropertyId to
                (1..numberOfEntries).map { RandomStringUtils.randomAscii(5) }.toSet())

        val entries1 = listOf(givenNames)
        val entries2 = listOf(givenNames)

        dataApi.createEntities(esId1, entries1)
        dataApi.createEntities(esId2, entries2)

        val ess = EntitySetSelection(Optional.of(setOf(personGivenNamePropertyId)), Optional.empty())

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(es1.name, es2.name))) {
            Thread.sleep(2000)
        }

        val data = ImmutableList.copyOf(dataApi.loadEntitySetData(esLinked.id, ess, FileType.json))

        //Remove the extra properties for easier equals.
        data.forEach {
            it.removeAll(DataTables.ID_FQN)
            it.removeAll(DataTables.LAST_INDEX_FQN)
            it.removeAll(DataTables.LAST_WRITE_FQN)
        }

        Assert.assertEquals(
                givenNames[personGivenNamePropertyId],
                data.first()[FullQualifiedName(PERSON_GIVEN_NAME_NAMESPACE, PERSON_GIVEN_NAME_NAME)]
        )
    }

    @Test
    fun testGetLinkedEntitySetsWithLinkingIds() {
        val personEntityTypeId = edmApi.getEntityTypeId(PERSON_NAMESPACE, PERSON_NAME)
        val personEt = edmApi.getEntityType(personEntityTypeId)

        // these ids are put into the whitelist configuration in indexer > linking.yaml
        // otherwise it will never get picked up by linking
        val esId1 = UUID.fromString("65847f7b-c939-414f-bb45-863cd594e412")
        val esId2 = UUID.fromString("8c97bf63-ca7a-4f37-ba1c-40338b1032d4")

        val es1 = try {
            createEntitySet(esId1, personEt)
        } catch (e: Exception) {
            edmApi.getEntitySet(esId1)
        }
        val es2 = try {
            createEntitySet(esId2, personEt)
        } catch (e: Exception) {
            edmApi.getEntitySet(esId2)
        }

        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Soft)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Hard)

        val esLinked = createEntitySet(personEt, true, setOf(esId1, esId2))

        val personGivenNamePropertyId = edmApi.getPropertyTypeId(PERSON_GIVEN_NAME_NAMESPACE, PERSON_GIVEN_NAME_NAME)
        val givenNames = mapOf(personGivenNamePropertyId to
                (1..numberOfEntries).map { RandomStringUtils.randomAscii(5) }.toSet())

        val entries1 = listOf(givenNames)
        val entries2 = listOf(givenNames)

        dataApi.createEntities(esId1, entries1)
        dataApi.createEntities(esId2, entries2)

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(es1.name, es2.name))) {
            Thread.sleep(3000)
        }

        val data = ImmutableList.copyOf(
                dataApi.loadEntitySetData(
                        esLinked.id,
                        EntitySetSelection(Optional.of(setOf(personGivenNamePropertyId)), Optional.empty()),
                        FileType.json
                )
        )
        val linkingIds = data.map { UUID.fromString(it[DataTables.ID_FQN].first() as String) }
        val indexedData = index(data)

        val personGivenNameFqn = FullQualifiedName(PERSON_GIVEN_NAME_NAMESPACE, PERSON_GIVEN_NAME_NAME)
        linkingIds.forEach {
            val ess = EntitySetSelection(Optional.of(setOf(personGivenNamePropertyId)), Optional.of(setOf(it)))
            val linkedEntity = ImmutableList.copyOf(dataApi.loadEntitySetData(esLinked.id, ess, FileType.json))

            Assert.assertArrayEquals(
                    arrayOf(indexedData[it]?.get(personGivenNameFqn)),
                    arrayOf(linkedEntity.first().get(personGivenNameFqn))
            )
        }
    }

    private fun index(
            data: Collection<SetMultimap<FullQualifiedName, Any>>
    ): Map<UUID, SetMultimap<FullQualifiedName, Any>> {
        return data.map {
            UUID.fromString(it[DataTables.ID_FQN].first() as String) to it
        }.toMap()
    }
}
