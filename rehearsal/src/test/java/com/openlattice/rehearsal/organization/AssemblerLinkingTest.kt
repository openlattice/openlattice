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
package com.openlattice.rehearsal.organization

import com.google.common.collect.ImmutableList
import com.openlattice.authorization.*
import com.openlattice.data.DataEdgeKey
import com.openlattice.data.DeleteType
import com.openlattice.data.EntityDataKey
import com.openlattice.data.UpdateType
import com.openlattice.data.requests.EntitySetSelection
import com.openlattice.data.requests.FileType
import com.openlattice.edm.EdmConstants
import com.openlattice.edm.EntitySet
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.edm.type.EntityType
import com.openlattice.mapstores.TestDataFactory.fqn
import com.openlattice.mapstores.TestDataFactory.randomStringEntityData
import com.openlattice.organization.Organization
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.rehearsal.SetupTestData
import com.openlattice.rehearsal.assertException
import com.openlattice.rehearsal.edm.EdmTestConstants
import org.apache.commons.lang.RandomStringUtils
import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.lang.reflect.UndeclaredThrowableException
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.util.*

private const val numberOfEntities = 10

class AssemblerLinkingTest : SetupTestData() {

    private val organizationDataSource = TestAssemblerConnectionManager.connect(organizationID)

    companion object {
        private lateinit var organization: Organization
        private lateinit var organizationID: UUID
        private lateinit var personEt: EntityType

        // use empty entity sets to be linked
        private val importedEntitySets = mapOf(
                "SocratesTestC" to Pair("socratesC.yaml", "emptyTestData.csv"),
                "SocratesTestD" to Pair("socratesD.yaml", "emptyTestData.csv")
        )

        @JvmStatic
        @BeforeClass
        fun init() {
            importedEntitySets.forEach {
                importDataSet(it.value.first, it.value.second)
            }

            while (!checkLinkingFinished(importedEntitySets.keys)) {
                Thread.sleep(5000L)
            }

            loginAs("admin")
            personEt = EdmTestConstants.personEt

            organization = createOrganization()
            organizationID = organization.id
        }

        @JvmStatic
        @AfterClass
        fun tearDown() {
            importedEntitySets.keys.forEach {
                try {
                    entitySetsApi.deleteEntitySet(entitySetsApi.getEntitySetId(it))
                } catch (e: UndeclaredThrowableException) {
                }
            }
        }
    }


    @Test
    fun testCreateMaterializedViews() {
        // clear normal entity sets and add data to them
        val esId1 = entitySetsApi.getEntitySetId(importedEntitySets.keys.first())
        val esId2 = entitySetsApi.getEntitySetId(importedEntitySets.keys.last())

        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Soft)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Hard)

        val esLinking = createEntitySet(personEt, true, setOf(esId1, esId2))

        val givenNames = (1..numberOfEntities).map {
            mapOf(EdmTestConstants.personGivenNameId to setOf(RandomStringUtils.randomAscii(5)))
        }

        dataApi.createEntities(esId1, givenNames)
        dataApi.createEntities(esId2, givenNames)


        // materialize linking entity set
        grantMaterializePermissions(organization, esLinking, personEt.properties)
        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to 2))

        Assert.assertTrue(
                organizationsApi
                        .getOrganizationEntitySets(organizationID, EnumSet.of(OrganizationEntitySetFlag.MATERIALIZED))
                        .keys.contains(esLinking.id)
        )
    }

    @Test
    fun testEdmUnsync() {
        // clear normal entity sets and create linking entity set
        val esId1 = entitySetsApi.getEntitySetId(importedEntitySets.keys.first())
        val esId2 = entitySetsApi.getEntitySetId(importedEntitySets.keys.last())

        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Soft)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Hard)

        val esLinking1 = createEntitySet(personEt, true, setOf(esId1, esId2))

        // materialize linking entity set, no refresh
        grantMaterializePermissions(organization, esLinking1, personEt.properties)
        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking1.id to null))

        // add property type
        val newPropertyType = createPropertyType()
        edmApi.addPropertyTypeToEntityType(personEt.id, newPropertyType.id)
        Assert.assertTrue(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking1.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )

        // wait to re-create view
        Thread.sleep(2000)

        grantMaterializePermissions(organization, esLinking1, setOf(newPropertyType.id))
        organizationsApi.synchronizeEdmChanges(organizationID, esLinking1.id)
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking1.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )

        // check if new column is there
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking1.name))
                Assert.assertTrue(
                        TestAssemblerConnectionManager.getColumnNames(rs)
                                .contains(newPropertyType.type.fullQualifiedNameAsString)
                )
            }
        }


        // create linking entity set
        val esLinking2 = createEntitySet(personEt, true, setOf(esId1, esId2))

        // materialize entity set, no refresh
        grantMaterializePermissions(organization, esLinking2, personEt.properties)
        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking2.id to null))

        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking2.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )

        // change property type fqn
        val propertyToChange = newPropertyType.id
        val newFqn = fqn()
        edmApi.updatePropertyTypeMetadata(
                propertyToChange,
                MetadataUpdate(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(newFqn),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()
                )
        )
        Assert.assertTrue(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking2.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )

        // wait to re-create view
        Thread.sleep(2000)

        organizationsApi.synchronizeEdmChanges(organizationID, esLinking2.id)
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking2.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )

        // check if column has new name
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking2.name))
                val columnNames = TestAssemblerConnectionManager.getColumnNames(rs)
                Assert.assertTrue(columnNames.contains(newFqn.fullQualifiedNameAsString))
                Assert.assertFalse(columnNames.contains(newPropertyType.type.fullQualifiedNameAsString))
            }
        }
    }

    @Test
    fun testAddAndRemoveLinkedEntitySets() {
        // clear normal entity sets and create linking entity set
        val esId1 = entitySetsApi.getEntitySetId(importedEntitySets.keys.first())
        val esId2 = entitySetsApi.getEntitySetId(importedEntitySets.keys.last())

        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Soft)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Hard)
        var esLinking = createEntitySet(personEt, true, setOf(esId1))

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(importedEntitySets.keys.first(), importedEntitySets.keys.last()))) {
            Thread.sleep(2000)
        }

        // add some data
        val givenNames1 = (1..numberOfEntities).map {
            mapOf(EdmTestConstants.personGivenNameId to setOf(RandomStringUtils.randomAscii(5)))
        }
        val givenNames2 = (1..numberOfEntities).map {
            mapOf(EdmTestConstants.personGivenNameId to setOf(RandomStringUtils.randomAscii(5)))
        }
        dataApi.createEntities(esId1, givenNames1)
        dataApi.createEntities(esId2, givenNames2)

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(importedEntitySets.keys.first(), importedEntitySets.keys.last()))) {
            Thread.sleep(2000)
        }

        // get linking ids
        val ess = EntitySetSelection(Optional.of(personEt.properties))
        val data1 = ImmutableList.copyOf(dataApi.loadEntitySetData(esLinking.id, ess, FileType.json))
        val linkingIds1 = data1.map { UUID.fromString(it[EdmConstants.ID_FQN].first() as String) }.toSet()

        // materialize entity set with all it's properties, no refresh
        grantMaterializePermissions(organization, esLinking, personEt.properties)
        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to null))

        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )

        // check linking ids
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name))

                val actualLinkingIds = mutableSetOf<UUID>()
                while (rs.next()) {
                    actualLinkingIds.add(ResultSetAdapters.linkingId(rs))
                }
                Assert.assertEquals(linkingIds1, actualLinkingIds)
            }
        }


        // add entity set to linking entity set
        entitySetsApi.addEntitySetsToLinkingEntitySet(esLinking.id, setOf(esId2))
        esLinking = entitySetsApi.getEntitySet(esLinking.id)

        Assert.assertTrue(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )

        // grant materialize again with new entity set included
        grantMaterializePermissions(organization, esLinking, personEt.properties)

        // refresh
        organizationsApi.synchronizeEdmChanges(organizationID, esLinking.id)
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )

        // check linking ids
        val data12 = ImmutableList.copyOf(dataApi.loadEntitySetData(esLinking.id, ess, FileType.json))
        val linkingIds12 = data12.map { UUID.fromString(it[EdmConstants.ID_FQN].first() as String) }.toSet()

        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name))
                val actualLinkingIds = mutableSetOf<UUID>()
                while (rs.next()) {
                    actualLinkingIds.add(ResultSetAdapters.linkingId(rs))
                }
                Assert.assertEquals(linkingIds12, actualLinkingIds)
            }
        }


        // remove entity set from linking entity set
        entitySetsApi.removeEntitySetsFromLinkingEntitySet(esLinking.id, setOf(esId1))
        esLinking = entitySetsApi.getEntitySet(esLinking.id)

        Assert.assertTrue(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )

        // grant materialize again with new entity set included
        grantMaterializePermissions(organization, esLinking, personEt.properties)

        // refresh
        organizationsApi.synchronizeEdmChanges(organizationID, esLinking.id)
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )

        // check linking ids
        val data2 = ImmutableList.copyOf(dataApi.loadEntitySetData(esLinking.id, ess, FileType.json))
        val linkingIds2 = data2.map { UUID.fromString(it[EdmConstants.ID_FQN].first() as String) }.toSet()

        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name))
                val actualLinkingIds = mutableSetOf<UUID>()
                while (rs.next()) {
                    actualLinkingIds.add(ResultSetAdapters.linkingId(rs))
                }
                Assert.assertEquals(linkingIds2, actualLinkingIds)
            }
        }
    }

    @Test
    fun testDataUnsync() {
        // clear normal entity sets and create linking entity set
        val esId1 = entitySetsApi.getEntitySetId(importedEntitySets.keys.first())
        val esId2 = entitySetsApi.getEntitySetId(importedEntitySets.keys.last())

        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Hard)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Soft)

        val esLinking = createEntitySet(personEt, true, setOf(esId1, esId2))
        val propertyFqns = personEt.properties.map { edmApi.getPropertyType(it).type }.toSet()

        // materialize entity set with all it's properties, no refresh
        grantMaterializePermissions(organization, esLinking, personEt.properties)
        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to null))

        // data is not supposed to be there, only the columns
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name))
                // all columns are there
                val columns = TestAssemblerConnectionManager.getColumnNames(rs)
                propertyFqns.forEach {
                    Assert.assertTrue(columns.contains(it.fullQualifiedNameAsString))
                }
                // no data is there yet
                Assert.assertFalse(rs.next())
            }
        }

        // add data
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )

        val givenNames = (1..numberOfEntities).map {
            mapOf(EdmTestConstants.personGivenNameId to setOf(RandomStringUtils.randomAscii(5)))
        }
        val ids1 = dataApi.createEntities(esId1, givenNames)
        val ids2 = dataApi.createEntities(esId2, givenNames)

        Assert.assertTrue(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(importedEntitySets.keys.first(), importedEntitySets.keys.last()))) {
            Thread.sleep(2000)
        }

        //  read data with linking ids
        val ess = EntitySetSelection(Optional.of(setOf(EdmTestConstants.personGivenNameId)))
        val loadedDataWithLinkingId = dataApi.loadEntitySetData(esLinking.id, ess, FileType.json).map {
            val values = it.asMap()
            val id = UUID.fromString(it[EdmConstants.ID_FQN].first() as String)
            values.remove(EdmConstants.ID_FQN)

            id to values
        }.toMap()

        // refresh
        organizationsApi.refreshDataChanges(organizationID, esLinking.id)
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )

        // check if data is in org database
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name))

                val materializedLinkingIds = mutableSetOf<UUID>()
                Assert.assertTrue(rs.next())

                do {
                    val entitySetId = ResultSetAdapters.entitySetId(rs)
                    val id = ResultSetAdapters.linkingId(rs)
                    Assert.assertTrue(esLinking.linkedEntitySets.contains(entitySetId))
                    Assert.assertTrue(loadedDataWithLinkingId.keys.contains(id))
                    Assert.assertEquals(
                            loadedDataWithLinkingId
                                    .getValue(id).getValue(EdmTestConstants.personGivenNameFqn).first() as String,
                            getStringResult(rs, EdmTestConstants.personGivenNameFqn.fullQualifiedNameAsString)
                    )

                    materializedLinkingIds.add(id)

                } while (rs.next())

                Assert.assertEquals(loadedDataWithLinkingId.keys, materializedLinkingIds)
            }
        }


        // update data
        val newGivenNames = (1..numberOfEntities).map {
            mapOf(EdmTestConstants.personGivenNameId to setOf(RandomStringUtils.randomAscii(5)))
        }
        val newTestDataWithIds1 = ids1.zip(newGivenNames).toMap()
        val newTestDataWithIds2 = ids2.zip(newGivenNames).toMap()
        dataApi.updateEntitiesInEntitySet(esId1, newTestDataWithIds1, UpdateType.Replace)
        dataApi.updateEntitiesInEntitySet(esId2, newTestDataWithIds2, UpdateType.Replace)

        Assert.assertTrue(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(importedEntitySets.keys.first(), importedEntitySets.keys.last()))) {
            Thread.sleep(2000)
        }

        val newLoadedDataWithLinkingId = dataApi.loadEntitySetData(esLinking.id, ess, FileType.json).map {
            val values = it.asMap()
            val id = UUID.fromString(it[EdmConstants.ID_FQN].first() as String)
            values.remove(EdmConstants.ID_FQN)

            id to values
        }.toMap()

        // refresh
        organizationsApi.refreshDataChanges(organizationID, esLinking.id)
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )
        // check if data is updated in org database
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name))

                val materializedLinkingIds = mutableSetOf<UUID>()
                Assert.assertTrue(rs.next())

                do {
                    val id = ResultSetAdapters.linkingId(rs)
                    val entitySetId = ResultSetAdapters.entitySetId(rs)
                    Assert.assertTrue(esLinking.linkedEntitySets.contains(entitySetId))
                    Assert.assertTrue(newLoadedDataWithLinkingId.keys.contains(id))
                    Assert.assertEquals(
                            newLoadedDataWithLinkingId
                                    .getValue(id).getValue(EdmTestConstants.personGivenNameFqn).first() as String,
                            getStringResult(rs, EdmTestConstants.personGivenNameFqn.fullQualifiedNameAsString)
                    )

                    materializedLinkingIds.add(id)

                } while (rs.next())

                Assert.assertEquals(newLoadedDataWithLinkingId.keys, materializedLinkingIds)
            }
        }

        // delete data
        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Soft)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Hard)
        Assert.assertTrue(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(importedEntitySets.keys.first(), importedEntitySets.keys.last()))) {
            Thread.sleep(2000)
        }

        // refresh
        organizationsApi.refreshDataChanges(organizationID, esLinking.id)
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )
        // check if data is deleted in org database
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name))
                // no data is there
                Assert.assertFalse(rs.next())
            }
        }
    }

    @Test
    fun testMaterializeEdges() {
        // clear normal entity sets and create linking entity set
        val esId = entitySetsApi.getEntitySetId(importedEntitySets.keys.first())
        dataApi.deleteAllEntitiesFromEntitySet(esId, DeleteType.Hard)

        val esLinking = createEntitySet(personEt, true, setOf(esId))

        // create edge and dst entity sets
        val dst = createEntityType()
        val edge = createEdgeEntityType()

        val esDst = createEntitySet(dst)
        val esEdge = createEntitySet(edge)

        // create association type with defining src and dst entity types
        createAssociationType(edge, setOf(personEt), setOf(dst))

        // materialize linking entity set, no refresh
        grantMaterializePermissions(organization, esLinking, personEt.properties)
        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to null))

        // edges should be there but empty
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectEdgesOfEntitySetsSql())
                Assert.assertFalse(rs.next())
            }
        }

        // add data to edges
        val givenNames = (1..numberOfEntities).map {
            mapOf(EdmTestConstants.personGivenNameId to setOf(RandomStringUtils.randomAscii(5)))
        }
        val ids = dataApi.createEntities(esId, givenNames)

        val testDataDst = randomStringEntityData(numberOfEntities, dst.properties).values.toList()
        val idsDst = dataApi.createEntities(esDst.id, testDataDst)

        val testDataEdge = randomStringEntityData(numberOfEntities, edge.properties).values.toList()
        val idsEdge = dataApi.createEntities(esEdge.id, testDataEdge)

        val edges = ids.mapIndexed { index, _ ->
            DataEdgeKey(
                    EntityDataKey(esId, ids[index]),
                    EntityDataKey(esDst.id, idsDst[index]),
                    EntityDataKey(esEdge.id, idsEdge[index])
            )
        }.toSet()
        dataApi.createAssociations(edges)

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(importedEntitySets.keys.first()))) {
            Thread.sleep(2000)
        }

        // re-materialize linking entity set, no refresh
        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to null))

        // edges should be still empty, since we materialize only the linking entity set
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectEdgesOfEntitySetsSql())
                Assert.assertFalse(rs.next())
            }
        }
    }

    @Test
    fun testMaterializeAuthorizations() {
        // TODO: after automatic permission change handling, remove extra calls of re-materialization

        // create new organization
        val organization = createOrganization()
        val organizationID = organization.id

        // create entityset and 1 entity for both normal entity sets
        val esId1 = entitySetsApi.getEntitySetId(importedEntitySets.keys.first())
        val esId2 = entitySetsApi.getEntitySetId(importedEntitySets.keys.last())

        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Hard)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Soft)

        val esLinking = createEntitySet(personEt, true, setOf(esId1, esId2))

        val givenNames1 = listOf(
                mapOf(
                        EdmTestConstants.personGivenNameId to setOf(RandomStringUtils.randomAscii(5))
                )
        )
        val givenNames2 = listOf(
                mapOf(
                        EdmTestConstants.personGivenNameId to setOf(RandomStringUtils.randomAscii(5))
                )
        )
        dataApi.createEntities(esId1, givenNames1)
        dataApi.createEntities(esId2, givenNames2)


        // remove permission of user1 on imported entity sets
        val readPermission = EnumSet.of(Permission.READ)
        esLinking.linkedEntitySets.forEach { esId ->
            permissionsApi.updateAcl(
                    AclData(
                            Acl(AclKey(esId), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))),
                            Action.REMOVE
                    )
            )
            personEt.properties.forEach {
                permissionsApi.updateAcl(
                        AclData(
                                Acl(AclKey(esId, it), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))),
                                Action.REMOVE
                        )
                )
            }
        }

        // pattern to get for which normal entity set the permission error is thrown
        val arrayPattern = "EntitySet \\[(.*?)\\]".toRegex()


        // add user1 as member of organization
        OrganizationControllerCallHelper.addMemberToOrganization(organizationID, user1.id)

        // user is not owner of organization
        loginAs("user1")
        assertException(
                { organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to 5)) },
                "Object [$organizationID] is not accessible."
        )
        loginAs("admin")

        // org principal has no permission on linking entityset
        val organizationAcl = Acl(
                AclKey(organizationID),
                setOf(Ace(user1, EnumSet.of(Permission.OWNER), OffsetDateTime.MAX))
        )
        permissionsApi.updateAcl(AclData(organizationAcl, Action.ADD))

        loginAs("user1")
        assertException(
                { organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to 1000)) },
                "EntitySet [${esLinking.id}] is not accessible by organization principal"
        )
        loginAs("admin")


        // org principal has only permission on linking entityset
        val materializePermissions = EnumSet.of(Permission.MATERIALIZE)
        val linkingEsMaterializationAcl = Acl(
                AclKey(esLinking.id),
                setOf(Ace(organization.principal, materializePermissions, OffsetDateTime.MAX))
        )
        permissionsApi.updateAcl(AclData(linkingEsMaterializationAcl, Action.ADD))

        try {
            loginAs("user1")
            organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to 3))
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            val esUuid = arrayPattern.find(e.undeclaredThrowable.message!!)!!.groupValues[1]
            Assert.assertTrue(esLinking.linkedEntitySets.contains(UUID.fromString(esUuid)))
        } finally {
            loginAs("admin")
        }


        // org principal has permission on 1 normal entity set
        val es1MaterializationAcl = Acl(
                AclKey(esId1),
                setOf(Ace(organization.principal, materializePermissions, OffsetDateTime.MAX))
        )
        permissionsApi.updateAcl(AclData(es1MaterializationAcl, Action.ADD))

        loginAs("user1")
        assertException(
                { organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to 2341)) },
                "EntitySet [$esId2] is not accessible by organization principal"
        )
        loginAs("admin")


        // org principal has permission on both normal entity sets
        val es2MaterializationAcl = Acl(
                AclKey(esId2),
                setOf(Ace(organization.principal, materializePermissions, OffsetDateTime.MAX))
        )
        permissionsApi.updateAcl(AclData(es2MaterializationAcl, Action.ADD))

        loginAs("user1")
        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to 34))
        loginAs("admin")

        val organizationDataSource = TestAssemblerConnectionManager.connect(organizationID)
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name))
                Assert.assertEquals(PostgresColumn.ENTITY_SET_ID.name, rs.metaData.getColumnName(1))
                Assert.assertEquals(PostgresColumn.LINKING_ID.name, rs.metaData.getColumnName(2))
            }
        }


        // org principal has permission on 1 property for only 1 normal entity set
        val propertyTypeId = personEt.properties.random()
        val propertyType = edmApi.getPropertyType(propertyTypeId)
        val ptMaterializationAcl1 = Acl(
                AclKey(esId1, propertyTypeId),
                setOf(Ace(organization.principal, materializePermissions, OffsetDateTime.MAX))
        )
        permissionsApi.updateAcl(AclData(ptMaterializationAcl1, Action.ADD))

        loginAs("user1")
        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to 21))
        loginAs("admin")

        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name))
                Assert.assertEquals(PostgresColumn.ENTITY_SET_ID.name, rs.metaData.getColumnName(1))
                Assert.assertEquals(PostgresColumn.LINKING_ID.name, rs.metaData.getColumnName(2))
            }
        }


        // org princiapl has permission on 1 property for both normal entity sets
        val ptMaterializationAcl2 = Acl(
                AclKey(esId2, propertyTypeId),
                setOf(Ace(organization.principal, materializePermissions, OffsetDateTime.MAX))
        )
        permissionsApi.updateAcl(AclData(ptMaterializationAcl2, Action.ADD))

        loginAs("user1")
        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to 1))
        loginAs("admin")

        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name))
                Assert.assertEquals(PostgresColumn.ENTITY_SET_ID.name, rs.metaData.getColumnName(1))
                Assert.assertEquals(PostgresColumn.LINKING_ID.name, rs.metaData.getColumnName(2))
                Assert.assertEquals(propertyType.type.fullQualifiedNameAsString, rs.metaData.getColumnName(3))
            }
        }


        // org principal has permission on all properties on both normal entity sets
        personEt.properties.forEach {
            val acl1 = Acl(
                    AclKey(esId1, it),
                    setOf(Ace(organization.principal, materializePermissions, OffsetDateTime.MAX))
            )
            permissionsApi.updateAcl(AclData(acl1, Action.ADD))
            val acl2 = Acl(
                    AclKey(esId2, it),
                    setOf(Ace(organization.principal, materializePermissions, OffsetDateTime.MAX))
            )
            permissionsApi.updateAcl(AclData(acl2, Action.ADD))
        }

        loginAs("user1")
        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to 100))

        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name))
                Assert.assertEquals(PostgresColumn.ENTITY_SET_ID.name, rs.metaData.getColumnName(1))
                Assert.assertEquals(PostgresColumn.LINKING_ID.name, rs.metaData.getColumnName(2))
                val columnNames = TestAssemblerConnectionManager.getColumnNames(rs)
                personEt.properties.forEach {
                    Assert.assertTrue(columnNames.contains(edmApi.getPropertyType(it).type.fullQualifiedNameAsString))
                }
            }
        }


        // try to select from materialized view with user1 (no read permissions)
        val materializedViewAccount = principalApi.materializedViewAccount

        val connectionProperties = Properties()
        connectionProperties["jdbcUrl"] = "jdbc:postgresql://localhost:5432"
        connectionProperties["username"] = materializedViewAccount.username
        connectionProperties["password"] = materializedViewAccount.credential
        connectionProperties["maximumPoolSize"] = 5
        connectionProperties["connectionTimeout"] = 60000

        // connect with user1(simple member) credentials
        val user1OrganizationDataSource = TestAssemblerConnectionManager.connect(
                organizationID,
                Optional.of(connectionProperties)
        )

        user1OrganizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                assertException(
                        { stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name)) },
                        "permission denied for materialized view ${esLinking.name}"
                )
            }
        }


        // add read for user1 on entityset, but none of the normal entitysets and re-materialize
        loginAs("admin")
        val readPermissions = EnumSet.of(Permission.READ)
        val linkingEsReadAcl = Acl(AclKey(esLinking.id), setOf(Ace(user1, readPermissions, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(linkingEsReadAcl, Action.ADD))

        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to 9))

        user1OrganizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                assertException(
                        { stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name)) },
                        "permission denied for materialized view ${esLinking.name}"
                )
            }
        }


        // add read for user1 on 1 normal entityset and re-materialize
        val esReadAcl1 = Acl(AclKey(esId1), setOf(Ace(user1, readPermissions, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(esReadAcl1, Action.ADD))

        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to 432))

        user1OrganizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                assertException(
                        { stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name)) },
                        "permission denied for materialized view ${esLinking.name}"
                )
            }
        }


        // add read for user1 on both normal entitysets, but no properties and re-materialize
        val esReadAcl2 = Acl(AclKey(esId2), setOf(Ace(user1, readPermissions, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(esReadAcl2, Action.ADD))

        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to 32))

        // try to select all columns
        user1OrganizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                assertException(
                        { stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name)) },
                        "permission denied for materialized view ${esLinking.name}"
                )
            }
        }

        // try to select only columns, which the user is authorized for
        user1OrganizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                //  user1 can access edges, since it has permission to 1 entityset
                stmt.executeQuery(TestAssemblerConnectionManager.selectEdgesOfEntitySetsSql())
                val rs = stmt.executeQuery(
                        TestAssemblerConnectionManager.selectFromEntitySetSql(
                                esLinking.name, setOf(PostgresColumn.ENTITY_SET_ID.name, PostgresColumn.LINKING_ID.name)
                        )
                )
                Assert.assertEquals(PostgresColumn.ENTITY_SET_ID.name, rs.metaData.getColumnName(1))
                Assert.assertEquals(PostgresColumn.LINKING_ID.name, rs.metaData.getColumnName(2))
            }
        }


        // add read for user1 on 1 property but only for 1 normal entity set and re-materialize
        val ptReadAcl1 = Acl(AclKey(esId1, propertyTypeId), setOf(Ace(user1, readPermissions, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(ptReadAcl1, Action.ADD))

        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to 4))

        // try to select property column
        user1OrganizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                assertException(
                        {
                            stmt.executeQuery(
                                    TestAssemblerConnectionManager.selectFromEntitySetSql(
                                            esLinking.name, setOf(propertyType.type.fullQualifiedNameAsString)
                                    )
                            )
                        },
                        "permission denied for materialized view ${esLinking.name}"
                )
            }
        }


        // add read for user1 on 1 property for both normal entity set and re-materialize
        val ptReadAcl2 = Acl(AclKey(esId2, propertyTypeId), setOf(Ace(user1, readPermissions, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(ptReadAcl2, Action.ADD))

        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to 1))

        // try to select all columns
        user1OrganizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                assertException(
                        { stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name)) },
                        "permission denied for materialized view ${esLinking.name}"
                )
            }
        }

        // try to select only columns, which the user is authorized for
        user1OrganizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(
                        TestAssemblerConnectionManager
                                .selectFromEntitySetSql(
                                        esLinking.name,
                                        setOf(
                                                PostgresColumn.ENTITY_SET_ID.name,
                                                PostgresColumn.LINKING_ID.name,
                                                propertyType.type.fullQualifiedNameAsString
                                        )
                                )
                )
                Assert.assertEquals(PostgresColumn.ENTITY_SET_ID.name, rs.metaData.getColumnName(1))
                Assert.assertEquals(PostgresColumn.LINKING_ID.name, rs.metaData.getColumnName(2))
                Assert.assertEquals(propertyType.type.fullQualifiedNameAsString, rs.metaData.getColumnName(3))
            }
        }


        // add read for user1 on all properties on both normal entity sets and re-materialize
        personEt.properties.forEach {
            val acl1 = Acl(AclKey(esId1, it), setOf(Ace(user1, readPermissions, OffsetDateTime.MAX)))
            permissionsApi.updateAcl(AclData(acl1, Action.ADD))
            val acl2 = Acl(AclKey(esId2, it), setOf(Ace(user1, readPermissions, OffsetDateTime.MAX)))
            permissionsApi.updateAcl(AclData(acl2, Action.ADD))
        }

        organizationsApi.assembleEntitySets(organizationID, mapOf(esLinking.id to 321))

        user1OrganizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking.name))
                Assert.assertEquals(PostgresColumn.ENTITY_SET_ID.name, rs.metaData.getColumnName(1))
                Assert.assertEquals(PostgresColumn.LINKING_ID.name, rs.metaData.getColumnName(2))
                val columns = TestAssemblerConnectionManager.getColumnNames(rs)
                personEt.properties.forEach {
                    Assert.assertTrue(columns.contains(edmApi.getPropertyType(it).type.fullQualifiedNameAsString))
                }
            }
        }

        loginAs("admin")
    }


    /**
     * Add permission to materialize entity set and it's properties to organization principal
     */
    private fun grantMaterializePermissions(
            organization: Organization, linkingEntitySet: EntitySet, properties: Set<UUID>
    ) {
        val newPermissions = EnumSet.of(Permission.MATERIALIZE)

        val linkingEntitySetAcl = Acl(
                AclKey(linkingEntitySet.id),
                setOf(Ace(organization.principal, newPermissions, OffsetDateTime.MAX))
        )
        permissionsApi.updateAcl(AclData(linkingEntitySetAcl, Action.ADD))

        // add permissions on properties and normal entity sets
        linkingEntitySet.linkedEntitySets.forEach { entitySetId ->
            val entitySetAcl = Acl(
                    AclKey(entitySetId),
                    setOf(Ace(organization.principal, newPermissions, OffsetDateTime.MAX))
            )
            permissionsApi.updateAcl(AclData(entitySetAcl, Action.ADD))

            properties.forEach {
                val propertyTypeAcl = Acl(
                        AclKey(entitySetId, it),
                        setOf(Ace(organization.principal, newPermissions, OffsetDateTime.MAX))
                )
                permissionsApi.updateAcl(AclData(propertyTypeAcl, Action.ADD))
            }
        }
    }

    private fun getStringResult(rs: ResultSet, column: String): String {
        return PostgresArrays.getTextArray(rs, column)[0]
    }
}