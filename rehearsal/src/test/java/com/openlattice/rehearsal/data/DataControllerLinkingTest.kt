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

import com.google.common.collect.ImmutableList
import com.google.common.collect.SetMultimap
import com.openlattice.data.DeleteType
import com.openlattice.data.requests.EntitySetSelection
import com.openlattice.data.requests.FileType
import com.openlattice.edm.type.EntityType
import com.openlattice.postgres.DataTables
import com.openlattice.rehearsal.SetupTestData
import com.openlattice.rehearsal.edm.EdmTestConstants
import org.apache.commons.lang.RandomStringUtils
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.lang.reflect.UndeclaredThrowableException
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

private val numberOfEntries = 10

class DataControllerLinkingTest : SetupTestData() {
    companion object {
        private val importedEntitySets = mapOf(
                "SocratesTestC" to Pair("socratesC.yaml", "emptyTestData.csv"),
                "SocratesTestD" to Pair("socratesD.yaml", "emptyTestData.csv"))

        lateinit var personEt: EntityType

        @JvmStatic
        @BeforeClass
        fun init() {
            importedEntitySets.forEach {
                importDataSet(it.value.first, it.value.second)
            }

            Thread.sleep(10000L)
            while (!checkLinkingFinished(importedEntitySets.keys)) {
                Thread.sleep(5000L)
            }

            loginAs("admin")
            personEt = EdmTestConstants.personEt
        }

        @JvmStatic
        @AfterClass
        fun tearDown() {
            importedEntitySets.keys.forEach {
                try {
                    edmApi.deleteEntitySet(edmApi.getEntitySetId(it))
                } catch (e: UndeclaredThrowableException) {
                }
            }
        }
    }

    @Test
    fun testGetLinkedEntitySets() {
        val esId1 = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val esId2 = edmApi.getEntitySetId(importedEntitySets.keys.last())

        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Soft)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Hard)

        val esLinked = createEntitySet(personEt, true, setOf(esId1, esId2))

        val personGivenNamePropertyId = EdmTestConstants.personGivenNameId
        val givenNames = mapOf(personGivenNamePropertyId to
                (1..numberOfEntries).map { RandomStringUtils.randomAscii(5) }.toSet())

        val entries1 = listOf(givenNames)
        val entries2 = listOf(givenNames)

        dataApi.createEntities(esId1, entries1)
        dataApi.createEntities(esId2, entries2)

        val ess = EntitySetSelection(Optional.of(setOf(personGivenNamePropertyId)), Optional.empty())

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(importedEntitySets.keys.first(), importedEntitySets.keys.last()))) {
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
                data.first()[EdmTestConstants.personGivenNameFqn]
        )
    }

    @Test
    fun testGetLinkedEntitySetsWithLinkingIds() {
        val esId1 = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val esId2 = edmApi.getEntitySetId(importedEntitySets.keys.last())

        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Soft)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Hard)

        val esLinked = createEntitySet(personEt, true, setOf(esId1, esId2))

        val personGivenNamePropertyId = EdmTestConstants.personGivenNameId
        val givenNames = mapOf(personGivenNamePropertyId to
                (1..numberOfEntries).map { RandomStringUtils.randomAscii(5) }.toSet())

        val entries1 = listOf(givenNames)
        val entries2 = listOf(givenNames)

        dataApi.createEntities(esId1, entries1)
        dataApi.createEntities(esId2, entries2)

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(importedEntitySets.keys.first(), importedEntitySets.keys.last()))) {
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

        linkingIds.forEach {
            val ess = EntitySetSelection(Optional.of(setOf(personGivenNamePropertyId)), Optional.of(setOf(it)))
            val linkedEntity = ImmutableList.copyOf(dataApi.loadEntitySetData(esLinked.id, ess, FileType.json))

            Assert.assertArrayEquals(
                    arrayOf(indexedData[it]?.get(EdmTestConstants.personGivenNameFqn)),
                    arrayOf(linkedEntity.first().get(EdmTestConstants.personGivenNameFqn))
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
