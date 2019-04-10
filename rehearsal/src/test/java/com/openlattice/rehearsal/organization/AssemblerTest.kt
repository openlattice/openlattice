package com.openlattice.rehearsal.organization

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import com.openlattice.authorization.*
import com.openlattice.data.DataEdgeKey
import com.openlattice.data.DeleteType
import com.openlattice.data.EntityDataKey
import com.openlattice.data.UpdateType
import com.openlattice.edm.EntitySet
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.Organization
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import org.apache.commons.lang.RandomStringUtils
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.util.*

private const val numberOfEntities = 10

class AssemblerTest : MultipleAuthenticatedUsersBase() {

    private val organizationDataSource = TestAssemblerConnectionManager.connect(organizationID)

    companion object {
        private lateinit var organization: Organization
        private lateinit var organizationID: UUID
        private lateinit var organizationName: String

        @JvmStatic
        @BeforeClass
        fun init() {
            loginAs("admin")

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
    }


    @Test
    fun testCreateMaterializedViews() {
        // create data and edges
        val src = createEntityType()
        val dst = createEntityType()
        val edge = createEdgeEntityType()

        val esSrc = createEntitySet(src)
        val esDst = createEntitySet(dst)
        val esEdge = createEntitySet(edge)

        val testDataSrc = TestDataFactory.randomStringEntityData(numberOfEntities, src.properties)
        val testDataDst = TestDataFactory.randomStringEntityData(numberOfEntities, dst.properties)
        val testDataEdge = TestDataFactory.randomStringEntityData(numberOfEntities, edge.properties)

        val entitiesSrc = ImmutableList.copyOf(testDataSrc.values)
        val idsSrc = dataApi.createEntities(esSrc.id, entitiesSrc)

        val entitiesDst = ImmutableList.copyOf(testDataDst.values)
        val idsDst = dataApi.createEntities(esDst.id, entitiesDst)

        val entitiesEdge = ImmutableList.copyOf(testDataEdge.values)
        val idsEdge = dataApi.createEntities(esEdge.id, entitiesEdge)

        val edges = idsSrc.mapIndexed { index, _ ->
            DataEdgeKey(
                    EntityDataKey(esSrc.id, idsSrc[index]),
                    EntityDataKey(esDst.id, idsDst[index]),
                    EntityDataKey(esEdge.id, idsEdge[index])
            )
        }.toSet()

        dataApi.createAssociations(edges)

        // add permission to src entity set and it's properties to organization principal for materialization
        grantMaterializePermissions(organization, esSrc, src.properties)

        // materialize src entity set
        organizationsApi.assembleEntitySets(organizationID, setOf(esSrc.id))

        Assert.assertTrue(organizationsApi
                .getOrganizationEntitySets(organizationID, EnumSet.of(OrganizationEntitySetFlag.MATERIALIZED))
                .keys.contains(esSrc.id))
    }

    @Test
    fun testEdmUnsync() {
        val et = createEntityType()
        val es1 = createEntitySet(et)

        // materialize entity set
        grantMaterializePermissions(organization, es1, setOf())
        organizationsApi.assembleEntitySets(organizationID, setOf(es1.id))
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es1.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))

        // add property type
        val pt = createPropertyType()
        edmApi.addPropertyTypeToEntityType(et.id, pt.id)
        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[es1.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))
        grantMaterializePermissions(organization, es1, setOf(pt.id))
        organizationsApi.synchronizeEdmChanges(organizationID, es1.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es1.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))
        // check if new column is there
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es1))
                Assert.assertTrue(rs.metaData.getColumnName(3) == pt.type.fullQualifiedNameAsString)
            }
        }


        val es2 = createEntitySet(et)
        // materialize entity set
        grantMaterializePermissions(organization, es2, et.properties)
        organizationsApi.assembleEntitySets(organizationID, setOf(es2.id))


        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es2.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))
        // change property type fqn
        val propertyToChange = et.properties.first()
        val newFqn = TestDataFactory.fqn()
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
        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[es2.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))
        organizationsApi.synchronizeEdmChanges(organizationID, es2.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es2.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))
        // check if column has new name
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager
                        .selectFromEntitySetSql(es2, setOf(edmApi.getPropertyType(propertyToChange))))
                Assert.assertTrue(rs.metaData.getColumnName(1) == newFqn.fullQualifiedNameAsString)
            }
        }
    }

    @Test
    fun testDataUnsync() {
        val et = createEntityType()
        val es = createEntitySet(et)
        val propertyFqns = edmApi.getPropertyTypesForEntitySet(es.id)
                .map { it.key to it.value.type.fullQualifiedNameAsString }.toMap()

        // materialize entity set with all it's properties
        grantMaterializePermissions(organization, es, et.properties)
        organizationsApi.assembleEntitySets(organizationID, setOf(es.id))
        // data is not supposed to be there, only the columns
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es))
                // all columns are there
                (1..rs.metaData.columnCount).forEach {
                    val columnName = rs.metaData.getColumnName(it)
                    if (columnName != PostgresColumn.ID.name && columnName != PostgresColumn.ENTITY_SET_ID.name) {
                        Assert.assertTrue(propertyFqns.values.contains(columnName))
                    }
                }
                // no data is there yet
                Assert.assertFalse(rs.next())
            }
        }

        // add data
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        val testData = TestDataFactory.randomStringEntityData(numberOfEntities, et.properties).values.toList()
        val ids = dataApi.createEntities(es.id, testData)
        val testDataWithIds = ids.zip(testData).toMap()

        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // refresh
        organizationsApi.refreshDataChanges(organizationID, es.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // check if data is in org database
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es))

                var index = 0
                Assert.assertTrue(rs.next())
                Assert.assertEquals(es.id, ResultSetAdapters.entitySetId(rs))
                do {
                    val id = ResultSetAdapters.id(rs)
                    Assert.assertTrue(ids.contains(id))
                    propertyFqns.forEach { propertyId, fqn ->
                        Assert.assertEquals(
                                testDataWithIds.getValue(id).getValue(propertyId).first(),
                                getStringResult(rs, fqn))
                    }
                    index++

                } while (rs.next())

                Assert.assertEquals(numberOfEntities, index)
            }
        }


        // update data
        val newTestData = TestDataFactory.randomStringEntityData(numberOfEntities, et.properties).values.toList()
        val newTestDataWithIds = ids.zip(newTestData).toMap()
        dataApi.updateEntitiesInEntitySet(es.id, newTestDataWithIds, UpdateType.Replace)
        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // refresh
        organizationsApi.refreshDataChanges(organizationID, es.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // check if data is updated in org database
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es))

                var index = 0
                Assert.assertTrue(rs.next())
                Assert.assertEquals(es.id, ResultSetAdapters.entitySetId(rs))
                do {
                    val id = ResultSetAdapters.id(rs)
                    Assert.assertTrue(ids.contains(id))
                    propertyFqns.forEach { propertyId, fqn ->
                        Assert.assertEquals(
                                newTestDataWithIds.getValue(id).getValue(propertyId).first(),
                                getStringResult(rs, fqn))
                    }
                    index++

                } while (rs.next())

                Assert.assertEquals(numberOfEntities, index)
            }
        }

        // delete data
        dataApi.deleteEntities(es.id, ids.toSet(), DeleteType.Hard)
        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // refresh
        organizationsApi.refreshDataChanges(organizationID, es.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // check if data is deleted in org database
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es))
                // no data is there
                Assert.assertFalse(rs.next())
            }
        }
    }

    @Test
    fun testMaterializeEdges() {
        // create entity sets
        val src = createEntityType()
        val dst = createEntityType()
        val edge = createEdgeEntityType()

        val esSrc = createEntitySet(src)
        val esDst = createEntitySet(dst)
        val esEdge = createEntitySet(edge)


        // grant permissions to materialize
        grantMaterializePermissions(organization, esSrc, src.properties)
        grantMaterializePermissions(organization, esEdge, edge.properties)

        // materialize src entity set
        organizationsApi.assembleEntitySets(organizationID, setOf(esSrc.id))

        // edges should be there but empty
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectEdgesOfEntitySetsSql())
                Assert.assertFalse(rs.next())
            }
        }


        // add data and edges
        val testDataSrc = TestDataFactory.randomStringEntityData(numberOfEntities, src.properties)
        val testDataDst = TestDataFactory.randomStringEntityData(numberOfEntities, dst.properties)
        val testDataEdge = TestDataFactory.randomStringEntityData(numberOfEntities, edge.properties)

        val entitiesSrc = ImmutableList.copyOf(testDataSrc.values)
        val idsSrc = dataApi.createEntities(esSrc.id, entitiesSrc)

        val entitiesDst = ImmutableList.copyOf(testDataDst.values)
        val idsDst = dataApi.createEntities(esDst.id, entitiesDst)

        val entitiesEdge = ImmutableList.copyOf(testDataEdge.values)
        val idsEdge = dataApi.createEntities(esEdge.id, entitiesEdge)

        val edges = idsSrc.mapIndexed { index, _ ->
            DataEdgeKey(
                    EntityDataKey(esSrc.id, idsSrc[index]),
                    EntityDataKey(esDst.id, idsDst[index]),
                    EntityDataKey(esEdge.id, idsEdge[index])
            )
        }.toSet()
        dataApi.createAssociations(edges)

        // re-materialize src entity set
        organizationsApi.assembleEntitySets(organizationID, setOf(esSrc.id))

        // edges should contain all ids
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectEdgesOfEntitySetsSql(setOf(esSrc.id)))

                var index = 0
                Assert.assertTrue(rs.next())
                do {
                    val edgeKey = ResultSetAdapters.edgeKey(rs)
                    Assert.assertTrue(edges.contains(edgeKey))

                    index++
                } while (rs.next())

                Assert.assertEquals(numberOfEntities, index)
            }
        }

        // materialize other entity set too
        organizationsApi.assembleEntitySets(organizationID, setOf(esEdge.id))

        // edges should contain same ids as before
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(
                        TestAssemblerConnectionManager.selectEdgesOfEntitySetsSql(setOf(esSrc.id, esEdge.id)))

                var index = 0
                Assert.assertTrue(rs.next())
                do {
                    val edgeKey = ResultSetAdapters.edgeKey(rs)
                    Assert.assertTrue(edges.contains(edgeKey))

                    index++
                } while (rs.next())

                Assert.assertEquals(numberOfEntities, index)
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
}