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
import com.google.common.collect.ImmutableSet
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
import com.openlattice.mapstores.TestDataFactory.fqn
import com.openlattice.mapstores.TestDataFactory.randomStringEntityData
import com.openlattice.organization.Organization
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.rehearsal.SetupTestData
import com.openlattice.rehearsal.edm.EdmTestConstants
import org.apache.commons.lang.RandomStringUtils
import org.apache.olingo.commons.api.edm.FullQualifiedName
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
        private lateinit var organizationName: String
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

            organizationName = RandomStringUtils.randomAlphanumeric(5)
            organization = Organization(
                    Optional.of(UUID.randomUUID()),
                    Principal(PrincipalType.ORGANIZATION, organizationName),
                    organizationName,
                    Optional.of("$organizationName description"),
                    ImmutableSet.of("tests1@openlattice.com"),
                    ImmutableSet.of(user1, user2, user3),
                    ImmutableSet.of(),
                    ImmutableSet.of(UUID.randomUUID()))

            //create organization
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

        val personGivenNamePropertyId = EdmTestConstants.personGivenNameId
        val givenNames = mapOf(personGivenNamePropertyId to
                (1..numberOfEntities).map { RandomStringUtils.randomAscii(5) }.toSet())

        val entries1 = listOf(givenNames)
        val entries2 = listOf(givenNames)

        dataApi.createEntities(esId1, entries1)
        dataApi.createEntities(esId2, entries2)


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
        grantMaterializePermissions(organization, esLinking1, setOf(newPropertyType.id))
        organizationsApi.synchronizeEdmChanges(organizationID, esLinking1.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking1.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))
        // check if new column is there
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking1))
                Assert.assertTrue(getColumnNames(rs).contains(newPropertyType.type.fullQualifiedNameAsString))
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
        organizationsApi.synchronizeEdmChanges(organizationID, esLinking2.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking2.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))

        // check if column has new name
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking2))
                val columnNames = getColumnNames(rs)
                Assert.assertTrue(columnNames.contains(newFqn.fullQualifiedNameAsString))
                Assert.assertFalse(columnNames.contains(newPropertyType.type.fullQualifiedNameAsString))
            }
        }
    }

    @Test
    fun testDataUnsync() {
        // clear normal entity sets and create linking entity set
        val esId1 = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val esId2 = edmApi.getEntitySetId(importedEntitySets.keys.last())

        dataApi.deleteAllEntitiesFromEntitySet(esId1, DeleteType.Soft)
        dataApi.deleteAllEntitiesFromEntitySet(esId2, DeleteType.Hard)

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
                (1..rs.metaData.columnCount).forEach {
                    val columnName = rs.metaData.getColumnName(it)
                    if (columnName != PostgresColumn.ID.name && columnName != PostgresColumn.ENTITY_SET_ID.name) {
                        Assert.assertTrue(propertyFqns.contains(FullQualifiedName(columnName)))
                    }
                }
                // no data is there yet
                Assert.assertFalse(rs.next())
            }
        }

        // add data
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))

        val givenNames = listOf(mapOf(EdmTestConstants.personGivenNameId to
                (1..numberOfEntities).map { RandomStringUtils.randomAscii(5) }.toSet()))
        val ids1 = dataApi.createEntities(esId1, givenNames)
        val ids2 = dataApi.createEntities(esId2, givenNames)

        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))

        // wait while linking finishes
        Thread.sleep(5000)
        while (!checkLinkingFinished(setOf(importedEntitySets.keys.first(), importedEntitySets.keys.last()))) {
            Thread.sleep(2000)
        }

        val ess = EntitySetSelection(Optional.of(setOf(EdmTestConstants.personGivenNameId)))
        val loadedDataWithLinkingId = dataApi.loadEntitySetData(esLinking.id, ess, FileType.json).map {
            val values = it.asMap()
            values.remove(DataTables.ID_FQN)

            UUID.fromString(it[DataTables.ID_FQN].first() as String) to values
        }.toMap()

        // refresh
        organizationsApi.refreshDataChanges(organizationID, esLinking.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // check if data is in org database
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking))

                var index = 0
                Assert.assertTrue(rs.next())
                Assert.assertEquals(esLinking.id, ResultSetAdapters.entitySetId(rs))
                do {
                    val id = ResultSetAdapters.id(rs)
                    Assert.assertTrue(loadedDataWithLinkingId.keys.contains(id))
                    propertyFqns.forEach {
                        Assert.assertEquals(
                                loadedDataWithLinkingId.getValue(id).getValue(it).first() as String,
                                getStringResult(rs, it.fullQualifiedNameAsString))
                    }
                    index++

                } while (rs.next())

                Assert.assertEquals(numberOfEntities, index)
            }
        }


        // update data
        val newGivenNames = listOf(mapOf(EdmTestConstants.personGivenNameId to
                (1..numberOfEntities).map { RandomStringUtils.randomAscii(5) }.toSet()))
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
            values.remove(DataTables.ID_FQN)

            UUID.fromString(it[DataTables.ID_FQN].first() as String) to values
        }.toMap()

        // refresh
        organizationsApi.refreshDataChanges(organizationID, esLinking.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[esLinking.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // check if data is updated in org database
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(esLinking))

                var index = 0
                Assert.assertTrue(rs.next())
                Assert.assertEquals(esLinking.id, ResultSetAdapters.entitySetId(rs))
                do {
                    val id = ResultSetAdapters.id(rs)
                    Assert.assertTrue(newLoadedDataWithLinkingId.keys.contains(id))
                    propertyFqns.forEach {
                        Assert.assertEquals(
                                newLoadedDataWithLinkingId.getValue(id).getValue(it).first(),
                                getStringResult(rs, it.fullQualifiedNameAsString))
                    }
                    index++

                } while (rs.next())

                Assert.assertEquals(numberOfEntities, index)
            }
        }

        // delete first half of data
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

    /**
     * Add permission to materialize entity set and it's properties to organization principal
     */
    private fun grantMaterializePermissions(organization: Organization, entitySet: EntitySet, properties: Set<UUID>) {
        val newPermissions = EnumSet.of(Permission.MATERIALIZE)
        val entitySetAcl = Acl(AclKey(entitySet.id), setOf(Ace(organization.principal, newPermissions, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(entitySetAcl, Action.ADD))

        // add permissions on properties
        properties.forEach {
            val propertyTypeAcl = Acl(
                    AclKey(entitySet.id, it),
                    setOf(Ace(organization.principal, newPermissions, OffsetDateTime.MAX)))
            permissionsApi.updateAcl(AclData(propertyTypeAcl, Action.ADD))
        }
    }

    private fun getStringResult(rs: ResultSet, column: String): String {
        return PostgresArrays.getTextArray(rs, column)[0]
    }

    private fun getColumnNames(rs: ResultSet): List<String> {
        return (1..rs.metaData.columnCount).map { column -> rs.metaData.getColumnName(column) }
    }
}