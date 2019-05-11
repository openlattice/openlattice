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
import com.openlattice.authorization.*
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
import java.time.OffsetDateTime
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

private const val numberOfEntries = 10

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

        val esLinking = createEntitySet(personEt, true, setOf(esId1, esId2))

        val personGivenNamePropertyId = EdmTestConstants.personGivenNameId
        val givenNames = (1..numberOfEntries).map {
            mapOf(personGivenNamePropertyId to setOf(RandomStringUtils.randomAscii(5)))
        }

        dataApi.createEntities(esId1, givenNames)
        dataApi.createEntities(esId2, givenNames)

        val ess = EntitySetSelection(Optional.of(setOf(personGivenNamePropertyId)), Optional.empty())

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(importedEntitySets.keys.first(), importedEntitySets.keys.last()))) {
            Thread.sleep(2000)
        }

        val data = ImmutableList.copyOf(dataApi.loadEntitySetData(esLinking.id, ess, FileType.json))

        //Remove the extra properties for easier equals.
        data.forEach {
            it.removeAll(DataTables.ID_FQN)
            it.removeAll(DataTables.LAST_INDEX_FQN)
            it.removeAll(DataTables.LAST_WRITE_FQN)
        }

        val actualGivenNamesData = data.flatMap { it[EdmTestConstants.personGivenNameFqn] }.toSet()
        val expectedGivenNamesData = givenNames.flatMap { it.getValue(EdmTestConstants.personGivenNameId) }.toSet()

        Assert.assertEquals(expectedGivenNamesData, actualGivenNamesData)
    }

    @Test
    fun testGetLinkedEntitySetsWithLinkingIds() {
        val esId1 = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val esId2 = edmApi.getEntitySetId(importedEntitySets.keys.last())

        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Soft)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Hard)

        val esLinking = createEntitySet(personEt, true, setOf(esId1, esId2))

        val personGivenNamePropertyId = EdmTestConstants.personGivenNameId
        val givenNames = (1..numberOfEntries).map {
            mapOf(personGivenNamePropertyId to setOf(RandomStringUtils.randomAscii(5)))
        }

        dataApi.createEntities(esId1, givenNames)
        dataApi.createEntities(esId2, givenNames)

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(importedEntitySets.keys.first(), importedEntitySets.keys.last()))) {
            Thread.sleep(3000)
        }

        val data = ImmutableList.copyOf(
                dataApi.loadEntitySetData(
                        esLinking.id,
                        EntitySetSelection(Optional.of(setOf(personGivenNamePropertyId)), Optional.empty()),
                        FileType.json
                )
        )
        val linkingIds = data.map { UUID.fromString(it[DataTables.ID_FQN].first() as String) }
        val indexedData = index(data)

        linkingIds.forEach {
            val ess = EntitySetSelection(Optional.of(setOf(personGivenNamePropertyId)), Optional.of(setOf(it)))
            val linkedEntity = ImmutableList.copyOf(dataApi.loadEntitySetData(esLinking.id, ess, FileType.json))

            Assert.assertArrayEquals(
                    arrayOf(indexedData[it]?.get(EdmTestConstants.personGivenNameFqn)),
                    arrayOf(linkedEntity.first().get(EdmTestConstants.personGivenNameFqn))
            )
        }
    }

    @Test
    fun testLoadDataAuthorizations() {
        // create data with admin
        val esId1 = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val esId2 = edmApi.getEntitySetId(importedEntitySets.keys.last())

        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Soft)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Hard)

        val esLinking = createEntitySet(personEt, true, setOf(esId1, esId2))

        val testData = (1..numberOfEntries).map {
            mapOf(EdmTestConstants.personGivenNameId to setOf(RandomStringUtils.randomAscii(5)),
                    EdmTestConstants.personMiddleNameId to setOf(RandomStringUtils.randomAscii(5)))
        }

        dataApi.createEntities(esId1, testData)
        dataApi.createEntities(esId2, testData)

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(importedEntitySets.keys.first(), importedEntitySets.keys.last()))) {
            Thread.sleep(3000)
        }

        val ess = EntitySetSelection(Optional.of(personEt.properties), Optional.empty())
        val data = ImmutableList.copyOf(dataApi.loadEntitySetData(esLinking.id, ess, FileType.json))
        val linkingIds = data.map { UUID.fromString(it[DataTables.ID_FQN].first() as String) }

        val arrayPattern = "Object \\[(.*?)\\]".toRegex()


        /* loadEntitySetData */

        // remove all permissions given to user1
        val readPermission = EnumSet.of(Permission.READ)
        val esReadAcl = Acl(AclKey(esLinking.id), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(esReadAcl, Action.REMOVE))
        esLinking.linkedEntitySets.forEach { esId ->
            permissionsApi.updateAcl(
                    AclData(
                            Acl(AclKey(esId), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))),
                            Action.REMOVE))
            personEt.properties.forEach {
                permissionsApi.updateAcl(
                        AclData(
                                Acl(AclKey(esId, it), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))),
                                Action.REMOVE))
            }
        }

        // try to read data with no permissions on it
        try {
            loginAs("user1")
            dataApi.loadEntitySetData(esLinking.id, ess, FileType.json)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Insufficient permissions to read the entity set ${esLinking.id} or it doesn't " +
                            "exists.", true))
        } finally {
            loginAs("admin")
        }

        // add permission to read entityset but none of the properties nor its normal entity sets
        permissionsApi.updateAcl(AclData(esReadAcl, Action.ADD))

        try {
            loginAs("user1")
            dataApi.loadEntitySetData(esLinking.id, ess, FileType.json)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            val esUuid = arrayPattern.find(e.undeclaredThrowable.message!!)!!.groupValues[1]
            Assert.assertTrue(esLinking.linkedEntitySets.contains(UUID.fromString(esUuid)))
        } finally {
            loginAs("admin")
        }


        // add permission to its normal entitysets but no properties
        esLinking.linkedEntitySets.forEach {
            val readAcl = Acl(AclKey(it), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
            permissionsApi.updateAcl(AclData(readAcl, Action.ADD))
        }

        loginAs("user1")
        val noData = ImmutableList.copyOf(dataApi.loadEntitySetData(esLinking.id, ess, FileType.json))
        Assert.assertEquals(linkingIds.size, noData.size)
        noData.forEach { Assert.assertEquals(setOf(DataTables.ID_FQN), it.asMap().keys) }
        loginAs("admin")


        // add permission on personGivenNamePropertyId to all its normal entitysets
        esLinking.linkedEntitySets.forEach {
            val givenNameReadAcl = Acl(
                    AclKey(it, EdmTestConstants.personGivenNameId),
                    setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
            permissionsApi.updateAcl(AclData(givenNameReadAcl, Action.ADD))
        }

        loginAs("user1")
        val givenNameData = ImmutableList.copyOf(dataApi.loadEntitySetData(esLinking.id, ess, FileType.json))
        Assert.assertEquals(linkingIds.size, givenNameData.size)
        givenNameData.forEach {
            Assert.assertEquals(setOf(DataTables.ID_FQN, EdmTestConstants.personGivenNameFqn), it.asMap().keys)
        }
        loginAs("admin")


        // add permission on all properties to all its normal entitysets
        esLinking.linkedEntitySets.forEach { esId ->
            personEt.properties.forEach {
                val ptReadAcl = Acl(AclKey(esId, it), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
                permissionsApi.updateAcl(AclData(ptReadAcl, Action.ADD))
            }
        }

        loginAs("user1")
        val dataAll = ImmutableList.copyOf(dataApi.loadEntitySetData(esLinking.id, ess, FileType.json))
        Assert.assertEquals(linkingIds.size, dataAll.size)
        dataAll.forEach {
            Assert.assertEquals(
                    setOf(DataTables.ID_FQN, EdmTestConstants.personGivenNameFqn, EdmTestConstants.personMiddleNameFqn),
                    it.asMap().keys)
        }
        loginAs("admin")


        /* getEntity */

        // remove all permissions given to user1
        permissionsApi.updateAcl(AclData(esReadAcl, Action.REMOVE))
        esLinking.linkedEntitySets.forEach { esId ->
            permissionsApi.updateAcl(
                    AclData(
                            Acl(AclKey(esId), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))),
                            Action.REMOVE))
            personEt.properties.forEach {
                permissionsApi.updateAcl(
                        AclData(
                                Acl(AclKey(esId, it), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))),
                                Action.REMOVE))
            }
        }

        val id = linkingIds.first()

        // try to read data with no permissions on it
        try {
            loginAs("user1")
            dataApi.getEntity(esLinking.id, id)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Object [${esLinking.id}] is not accessible.", true))
        }

        try {
            dataApi.getEntity(esLinking.id, id, EdmTestConstants.personGivenNameId)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Object [${esLinking.id}] is not accessible.", true))
        } finally {
            loginAs("admin")
        }

        // add permission to read linking entityset but none of the properties nor its normal entity sets
        val es2ReadAcl = Acl(AclKey(esLinking.id), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(es2ReadAcl, Action.ADD))

        try {
            loginAs("user1")
            dataApi.getEntity(esLinking.id, id)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            val esUuid = arrayPattern.find(e.undeclaredThrowable.message!!)!!.groupValues[1]
            Assert.assertTrue(esLinking.linkedEntitySets.contains(UUID.fromString(esUuid)))
        }

        try {
            dataApi.getEntity(esLinking.id, id, EdmTestConstants.personGivenNameId)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            val esUuid = arrayPattern.find(e.undeclaredThrowable.message!!)!!.groupValues[1]
            Assert.assertTrue(esLinking.linkedEntitySets.contains(UUID.fromString(esUuid)))
        } finally {
            loginAs("admin")
        }


        // add permission to its normal entity sets but no properties
        esLinking.linkedEntitySets.forEach {
            val readAcl = Acl(AclKey(it), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
            permissionsApi.updateAcl(AclData(readAcl, Action.ADD))
        }

        loginAs("user1")
        val noData2 = dataApi.getEntity(esLinking.id, id).asMap()
        Assert.assertEquals(1, noData2.size)
        noData2.forEach { Assert.assertEquals(DataTables.ID_FQN, it.key) }

        try {
            dataApi.getEntity(esLinking.id, id, EdmTestConstants.personGivenNameId)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Not authorized to read property type ${EdmTestConstants.personGivenNameId} in " +
                            "one or more normal entity sets of linking entity set ${esLinking.id}",
                            true))
        } finally {
            loginAs("admin")
        }


        // add permission on personGivenNamePropertyId
        esLinking.linkedEntitySets.forEach {
            val givenNameReadAcl = Acl(
                    AclKey(it, EdmTestConstants.personGivenNameId),
                    setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
            permissionsApi.updateAcl(AclData(givenNameReadAcl, Action.ADD))
        }


        loginAs("user1")

        val ptData1 = dataApi.getEntity(esLinking.id, id).asMap()
        Assert.assertEquals(1, ptData1[DataTables.ID_FQN]!!.size)
        Assert.assertEquals(setOf(DataTables.ID_FQN, EdmTestConstants.personGivenNameFqn), ptData1.keys)

        val ptData2 = dataApi.getEntity(esLinking.id, id, EdmTestConstants.personGivenNameId)
        Assert.assertEquals(1, ptData2.size)

        loginAs("admin")


        // add permission on all properties on all normal entity sets
        esLinking.linkedEntitySets.forEach { esId ->
            personEt.properties.forEach {
                permissionsApi.updateAcl(AclData(
                        Acl(AclKey(esId, it), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))), Action.ADD))
            }
        }

        loginAs("user1")

        val dataAll1 = dataApi.getEntity(esLinking.id, id).asMap()
        Assert.assertEquals(1, dataAll1[DataTables.ID_FQN]!!.size)
        Assert.assertEquals(
                setOf(DataTables.ID_FQN, EdmTestConstants.personGivenNameFqn, EdmTestConstants.personMiddleNameFqn),
                dataAll1.keys)

        val dataAll2 = dataApi.getEntity(esLinking.id, id, EdmTestConstants.personGivenNameId)
        Assert.assertEquals(1, dataAll2.size)

        loginAs("admin")
    }

    private fun index(
            data: Collection<SetMultimap<FullQualifiedName, Any>>
    ): Map<UUID, SetMultimap<FullQualifiedName, Any>> {
        return data.map {
            UUID.fromString(it[DataTables.ID_FQN].first() as String) to it
        }.toMap()
    }
}
