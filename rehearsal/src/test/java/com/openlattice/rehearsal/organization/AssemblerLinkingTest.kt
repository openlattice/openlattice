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
import com.openlattice.edm.EntitySet
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.edm.type.EntityType
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.mapstores.TestDataFactory.fqn
import com.openlattice.mapstores.TestDataFactory.randomStringEntityData
import com.openlattice.organization.Organization
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.rehearsal.SetupTestData
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
                "SocratesTestD" to Pair("socratesD.yaml", "emptyTestData.csv"))

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

            organization = TestDataFactory.organization()
            organizationID = organizationsApi.createOrganizationIfNotExists(organization)
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
    fun testCreateMaterializedViews() {
        // clear normal entity sets and add data to them
        val esId1 = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val esId2 = edmApi.getEntitySetId(importedEntitySets.keys.last())

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
        organizationsApi.assembleEntitySets(organizationID, setOf(esLinking.id))

        Assert.assertTrue(organizationsApi
                .getOrganizationEntitySets(organizationID, EnumSet.of(OrganizationEntitySetFlag.MATERIALIZED))
                .keys.contains(esLinking.id))
    }

    @Test
    fun testEdmUnsync() {
        // clear normal entity sets and create linking entity set
        val esId1 = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val esId2 = edmApi.getEntitySetId(importedEntitySets.keys.last())

        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Soft)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Hard)

        val esLinking1 = createEntitySet(personEt, true, setOf(esId1, esId2))

        // materialize linking entity set
        grantMaterializePermissions(organization, esLinking1, personEt.properties)
        organizationsApi.assembleEntitySets(organizationID, setOf(esLinking1.id))

        // add property type
        val newPropertyType = createPropertyType()
        edmApi.addPropertyTypeToEntityType(personEt.id, newPropertyType.id)
        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking1.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))

        // wait to re-create view
        Thread.sleep(2000)

        grantMaterializePermissions(organization, esLinking1, setOf(newPropertyType.id))
        organizationsApi.synchronizeEdmChanges(organizationID, esLinking1.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking1.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))

        // check if new column is there
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking1))
                Assert.assertTrue(TestAssemblerConnectionManager.getColumnNames(rs)
                        .contains(newPropertyType.type.fullQualifiedNameAsString))
            }
        }


        // create linking entity set
        val esLinking2 = createEntitySet(personEt, true, setOf(esId1, esId2))

        // materialize entity set
        grantMaterializePermissions(organization, esLinking2, personEt.properties)
        organizationsApi.assembleEntitySets(organizationID, setOf(esLinking2.id))

        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking2.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))

        // change property type fqn
        val propertyToChange = newPropertyType.id
        val newFqn = fqn()
        edmApi.updatePropertyTypeMetadata(
                propertyToChange,
                MetadataUpdate(Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(newFqn),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking2.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))

        // wait to re-create view
        Thread.sleep(2000)

        organizationsApi.synchronizeEdmChanges(organizationID, esLinking2.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking2.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))

        // check if column has new name
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking2))
                val columnNames = TestAssemblerConnectionManager.getColumnNames(rs)
                Assert.assertTrue(columnNames.contains(newFqn.fullQualifiedNameAsString))
                Assert.assertFalse(columnNames.contains(newPropertyType.type.fullQualifiedNameAsString))
            }
        }
    }

    @Test
    fun testAddAndRemoveLinkedEntitySets() {
        // clear normal entity sets and create linking entity set
        val esId1 = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val esId2 = edmApi.getEntitySetId(importedEntitySets.keys.last())

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
        val linkingIds1 = data1.map { UUID.fromString(it[DataTables.ID_FQN].first() as String) }.toSet()

        // materialize entity set with all it's properties
        grantMaterializePermissions(organization, esLinking, personEt.properties)
        organizationsApi.assembleEntitySets(organizationID, setOf(esLinking.id))

        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))

        // check linking ids
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking))

                val actualLinkingIds = mutableSetOf<UUID>()
                while (rs.next()) {
                    actualLinkingIds.add(ResultSetAdapters.linkingId(rs))
                }
                Assert.assertEquals(linkingIds1, actualLinkingIds)
            }
        }


        // add entity set to linking entity set
        entitySetsApi.addEntitySetsToLinkingEntitySet(esLinking.id, setOf(esId2))
        esLinking = edmApi.getEntitySet(esLinking.id)

        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))

        // grant materialize again with new entity set included
        grantMaterializePermissions(organization, esLinking, personEt.properties)

        // refresh
        organizationsApi.refreshDataChanges(organizationID, esLinking.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))

        // check linking ids
        val data12 = ImmutableList.copyOf(dataApi.loadEntitySetData(esLinking.id, ess, FileType.json))
        val linkingIds12 = data12.map { UUID.fromString(it[DataTables.ID_FQN].first() as String) }.toSet()

        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking))
                val actualLinkingIds = mutableSetOf<UUID>()
                while (rs.next()) {
                    actualLinkingIds.add(ResultSetAdapters.linkingId(rs))
                }
                Assert.assertEquals(linkingIds12, actualLinkingIds)
            }
        }


        // remove entity set from linking entity set
        entitySetsApi.removeEntitySetsFromLinkingEntitySet(esLinking.id, setOf(esId1))
        esLinking = edmApi.getEntitySet(esLinking.id)

        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))

        // grant materialize again with new entity set included
        grantMaterializePermissions(organization, esLinking, personEt.properties)

        // refresh
        organizationsApi.refreshDataChanges(organizationID, esLinking.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))

        // check linking ids
        val data2 = ImmutableList.copyOf(dataApi.loadEntitySetData(esLinking.id, ess, FileType.json))
        val linkingIds2 = data2.map { UUID.fromString(it[DataTables.ID_FQN].first() as String) }.toSet()

        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking))
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
        val esId1 = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val esId2 = edmApi.getEntitySetId(importedEntitySets.keys.last())

        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Hard)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Soft)

        val esLinking = createEntitySet(personEt, true, setOf(esId1, esId2))
        val propertyFqns = personEt.properties.map { edmApi.getPropertyType(it).type }.toSet()

        // materialize entity set with all it's properties
        grantMaterializePermissions(organization, esLinking, personEt.properties)
        organizationsApi.assembleEntitySets(organizationID, setOf(esLinking.id))

        // data is not supposed to be there, only the columns
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking))
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
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))

        val givenNames = (1..numberOfEntities).map {
            mapOf(EdmTestConstants.personGivenNameId to setOf(RandomStringUtils.randomAscii(5)))
        }
        val ids1 = dataApi.createEntities(esId1, givenNames)
        val ids2 = dataApi.createEntities(esId2, givenNames)

        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(importedEntitySets.keys.first(), importedEntitySets.keys.last()))) {
            Thread.sleep(2000)
        }

        //  read data with linking ids
        val ess = EntitySetSelection(Optional.of(setOf(EdmTestConstants.personGivenNameId)))
        val loadedDataWithLinkingId = dataApi.loadEntitySetData(esLinking.id, ess, FileType.json).map {
            val values = it.asMap()
            val id = UUID.fromString(it[DataTables.ID_FQN].first() as String)
            values.remove(DataTables.ID_FQN)

            id to values
        }.toMap()

        // refresh
        organizationsApi.refreshDataChanges(organizationID, esLinking.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))

        // check if data is in org database
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking))

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
                            getStringResult(rs, EdmTestConstants.personGivenNameFqn.fullQualifiedNameAsString))

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

        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(importedEntitySets.keys.first(), importedEntitySets.keys.last()))) {
            Thread.sleep(2000)
        }

        val newLoadedDataWithLinkingId = dataApi.loadEntitySetData(esLinking.id, ess, FileType.json).map {
            val values = it.asMap()
            val id = UUID.fromString(it[DataTables.ID_FQN].first() as String)
            values.remove(DataTables.ID_FQN)

            id to values
        }.toMap()

        // refresh
        organizationsApi.refreshDataChanges(organizationID, esLinking.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // check if data is updated in org database
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking))

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
                            getStringResult(rs, EdmTestConstants.personGivenNameFqn.fullQualifiedNameAsString))

                    materializedLinkingIds.add(id)

                } while (rs.next())

                Assert.assertEquals(newLoadedDataWithLinkingId.keys, materializedLinkingIds)
            }
        }

        // delete data
        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Soft)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Hard)
        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(importedEntitySets.keys.first(), importedEntitySets.keys.last()))) {
            Thread.sleep(2000)
        }

        // refresh
        organizationsApi.refreshDataChanges(organizationID, esLinking.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // check if data is deleted in org database
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking))
                // no data is there
                Assert.assertFalse(rs.next())
            }
        }
    }

    @Test
    fun testMaterializeEdges() {
        // clear normal entity sets and create linking entity set
        val esId = edmApi.getEntitySetId(importedEntitySets.keys.first())
        dataApi.deleteAllEntitiesFromEntitySet(esId, DeleteType.Hard)

        val esLinking = createEntitySet(personEt, true, setOf(esId))

        // create edge and dst entity sets
        val dst = createEntityType()
        val edge = createEdgeEntityType()

        val esDst = createEntitySet(dst)
        val esEdge = createEntitySet(edge)

        // materialize linking entity set
        grantMaterializePermissions(organization, esLinking, personEt.properties)
        organizationsApi.assembleEntitySets(organizationID, setOf(esLinking.id))

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

        // re-materialize linking entity set
        organizationsApi.assembleEntitySets(organizationID, setOf(esLinking.id))

        // edges should be still empty, since we materialize only the linking entity set
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectEdgesOfEntitySetsSql())
                Assert.assertFalse(rs.next())
            }
        }
    }

    /**
     * Add permission to materialize entity set and it's properties to organization principal
     */
    private fun grantMaterializePermissions(
            organization: Organization, linkingEntitySet: EntitySet, properties: Set<UUID>) {
        val newPermissions = EnumSet.of(Permission.MATERIALIZE)

        val linkingEntitySetAcl = Acl(
                AclKey(linkingEntitySet.id),
                setOf(Ace(organization.principal, newPermissions, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(linkingEntitySetAcl, Action.ADD))

        // add permissions on properties and normal entity sets
        linkingEntitySet.linkedEntitySets.forEach { entitySetId ->
            val entitySetAcl = Acl(
                    AclKey(entitySetId),
                    setOf(Ace(organization.principal, newPermissions, OffsetDateTime.MAX)))
            permissionsApi.updateAcl(AclData(entitySetAcl, Action.ADD))

            properties.forEach {
                val propertyTypeAcl = Acl(
                        AclKey(entitySetId, it),
                        setOf(Ace(organization.principal, newPermissions, OffsetDateTime.MAX)))
                permissionsApi.updateAcl(AclData(propertyTypeAcl, Action.ADD))
            }
        }
    }

    private fun getStringResult(rs: ResultSet, column: String): String {
        return PostgresArrays.getTextArray(rs, column)[0]
    }
}