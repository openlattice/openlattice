package com.openlattice.rehearsal.organization

import com.google.common.collect.ImmutableList
import com.openlattice.authorization.*
import com.openlattice.data.DataEdgeKey
import com.openlattice.data.DeleteType
import com.openlattice.data.EntityDataKey
import com.openlattice.data.UpdateType
import com.openlattice.data.storage.MetadataOption
import com.openlattice.edm.EntitySet
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.Organization
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.postgresql.util.PSQLException
import java.lang.reflect.UndeclaredThrowableException
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.util.*

private const val numberOfEntities = 10

class AssemblerTest : MultipleAuthenticatedUsersBase() {

    private val organizationDataSource = TestAssemblerConnectionManager.connect(organizationID)

    companion object {
        private lateinit var organization: Organization
        private lateinit var organizationID: UUID

        @JvmStatic
        @BeforeClass
        fun init() {
            loginAs("admin")

            //create organization
            organization = TestDataFactory.organization()
            organizationID = organizationsApi.createOrganizationIfNotExists(organization)
        }
    }
    // todo: test for changing refresh rate
    // todo: test for automatic refresh


    @Test
    fun testCreateMaterializedViews() {
        // create data and edges
        val src = createEntityType()
        val dst = createEntityType()
        val edge = createEdgeEntityType()

        val esSrc = createEntitySet(src)
        val esDst = createEntitySet(dst)
        val esEdge = createEntitySet(edge)

        // create association type with defining src and dst entity types
        createAssociationType(edge, setOf(src), setOf(dst))

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
        organizationsApi.assembleEntitySets(organizationID, mapOf(esSrc.id to 1))

        Assert.assertTrue(
                organizationsApi
                        .getOrganizationEntitySets(organizationID, EnumSet.of(OrganizationEntitySetFlag.MATERIALIZED))
                        .keys.contains(esSrc.id)
        )
    }

    @Test
    fun testEdmUnsync() {
        val et = createEntityType()
        val es1 = createEntitySet(et)

        // materialize entity set, no automatic refresh
        grantMaterializePermissions(organization, es1, setOf())
        organizationsApi.assembleEntitySets(organizationID, mapOf(es1.id to null))
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[es1.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )


        // add property type
        val pt = createPropertyType()
        edmApi.addPropertyTypeToEntityType(et.id, pt.id)
        Assert.assertTrue(
                organizationsApi.getOrganizationEntitySets(organizationID)[es1.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )
        grantMaterializePermissions(organization, es1, setOf(pt.id))
        // sync edm changes
        organizationsApi.synchronizeEdmChanges(organizationID, es1.id)
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[es1.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )
        // check if new column is there
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es1))
                Assert.assertEquals(pt.type.fullQualifiedNameAsString, rs.metaData.getColumnName(4))
            }
        }


        val es2 = createEntitySet(et)
        // materialize entity set, no automatic refresh
        grantMaterializePermissions(organization, es2, et.properties)
        organizationsApi.assembleEntitySets(organizationID, mapOf(es2.id to null))


        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[es2.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )
        // change property type fqn
        val propertyToChange = et.properties.first()
        val newFqn = TestDataFactory.fqn()
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
                        Optional.empty()
                )
        )
        Assert.assertTrue(
                organizationsApi.getOrganizationEntitySets(organizationID)[es2.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )
        organizationsApi.synchronizeEdmChanges(organizationID, es2.id)
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[es2.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )
        // check if column has new name
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(
                        TestAssemblerConnectionManager.selectFromEntitySetSql(
                                es2,
                                setOf(edmApi.getPropertyType(propertyToChange).type.fullQualifiedNameAsString)
                        )
                )
                Assert.assertEquals(newFqn.fullQualifiedNameAsString, rs.metaData.getColumnName(1))
            }
        }


        // remove property type from entity type
        edmApi.forceRemovePropertyTypeFromEntityType(et.id, pt.id)

        Assert.assertTrue(
                organizationsApi.getOrganizationEntitySets(organizationID)[es1.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )
        grantMaterializePermissions(organization, es1, setOf(pt.id))
        // sync edm changes
        organizationsApi.synchronizeEdmChanges(organizationID, es1.id)
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[es1.id]!!
                        .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
        )
        // check if old column is deleted
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es1))
                Assert.assertEquals(3, rs.metaData.columnCount)
            }
        }
    }

    @Test
    fun testDataUnsync() {
        val et = createEntityType()
        val es = createEntitySet(et)
        val propertyFqns = entitySetsApi.getPropertyTypesForEntitySet(es.id)
                .map { it.key to it.value.type.fullQualifiedNameAsString }.toMap()

        // materialize entity set with all it's properties and no automatic refresh
        grantMaterializePermissions(organization, es, et.properties)
        organizationsApi.assembleEntitySets(organizationID, mapOf(es.id to null))
        // data is not supposed to be there, only the columns
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es))
                // all columns are there
                (1..rs.metaData.columnCount).forEach {
                    val columnName = rs.metaData.getColumnName(it)
                    if (columnName != PostgresColumn.ID.name && columnName != PostgresColumn.ENTITY_SET_ID.name
                            && columnName != ResultSetAdapters
                                    .mapMetadataOptionToPostgresColumn(MetadataOption.ENTITY_KEY_IDS)) {
                        Assert.assertTrue(propertyFqns.values.contains(columnName))
                    }
                }
                // no data is there yet
                Assert.assertFalse(rs.next())
            }
        }

        // add data
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )
        val testData = TestDataFactory.randomStringEntityData(numberOfEntities, et.properties).values.toList()
        val ids = dataApi.createEntities(es.id, testData)
        val testDataWithIds = ids.zip(testData).toMap()

        Assert.assertTrue(
                organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )
        // refresh
        organizationsApi.refreshDataChanges(organizationID, es.id)
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )
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
                                getStringResult(rs, fqn)
                        )
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
        Assert.assertTrue(
                organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )
        // refresh
        organizationsApi.refreshDataChanges(organizationID, es.id)
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )
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
                                getStringResult(rs, fqn)
                        )
                    }
                    index++

                } while (rs.next())

                Assert.assertEquals(numberOfEntities, index)
            }
        }

        // delete data
        dataApi.deleteEntities(es.id, ids.toSet(), DeleteType.Hard)
        Assert.assertTrue(
                organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )
        // refresh
        organizationsApi.refreshDataChanges(organizationID, es.id)
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )
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
        // create new organization to test empty edges
        val organization = TestDataFactory.organization()
        val organizationID = organizationsApi.createOrganizationIfNotExists(organization)
        val organizationDataSource = TestAssemblerConnectionManager.connect(organizationID)

        // create entity sets
        val src = createEntityType()
        val dst = createEntityType()
        val edge = createEdgeEntityType()

        val esSrc = createEntitySet(src)
        val esDst = createEntitySet(dst)
        val esEdge = createEntitySet(edge)

        // create association type with defining src and dst entity types
        createAssociationType(edge, setOf(src), setOf(dst))

        // grant permissions to materialize
        grantMaterializePermissions(organization, esSrc, src.properties)
        grantMaterializePermissions(organization, esEdge, edge.properties)

        // materialize src entity set
        organizationsApi.assembleEntitySets(organizationID, mapOf(esSrc.id to null))

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
        organizationsApi.assembleEntitySets(organizationID, mapOf(esSrc.id to null))

        // edges should contain all ids
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(
                        TestAssemblerConnectionManager.selectEdgesOfEntitySetsSql(
                                Optional.of(esSrc.id),
                                Optional.of(esEdge.id),
                                Optional.of(esDst.id)
                        )
                )

                var index = 0
                Assert.assertTrue(rs.next())
                do {
                    val edgeKey = ResultSetAdapters.edgeKey(rs)
                    Assert.assertTrue(edges.contains(edgeKey))

                    index++
                } while (rs.next())

                Assert.assertEquals(numberOfEntities * 3, index)
            }
        }

        // materialize other entity set too
        organizationsApi.assembleEntitySets(organizationID, mapOf(esEdge.id to null))

        // edges should contain same ids as before
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(
                        TestAssemblerConnectionManager.selectEdgesOfEntitySetsSql(
                                Optional.of(esSrc.id),
                                Optional.of(esEdge.id),
                                Optional.of(esDst.id)
                        )
                )

                var index = 0
                Assert.assertTrue(rs.next())
                do {
                    val edgeKey = ResultSetAdapters.edgeKey(rs)
                    Assert.assertTrue(edges.contains(edgeKey))

                    index++
                } while (rs.next())

                Assert.assertEquals(numberOfEntities * 3, index)
            }
        }

    }

    @Test
    fun testMaterializeAuthorizations() {
        // TODO: after automatic permission change handling, remove extra calls of re-materialization

        // create new organization
        val organization = TestDataFactory.organization()
        val organizationID = organizationsApi.createOrganizationIfNotExists(organization)

        // create entityset and entities
        val et = createEntityType()
        val es = createEntitySet(et)

        val testData = TestDataFactory.randomStringEntityData(numberOfEntities, et.properties).values.toList()
        dataApi.createEntities(es.id, testData).toSet().zip(testData).toMap()

        // add user1 as member of organization
        OrganizationControllerCallHelper.addMemberToOrganization(organizationID, user1.id)

        // user is not owner of organization
        try {
            loginAs("user1")
            organizationsApi.assembleEntitySets(organizationID, mapOf(es.id to 100))
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(
                    e.undeclaredThrowable.message!!.contains(
                            "Object [$organizationID] is not accessible.", true
                    )
            )
        } finally {
            loginAs("admin")
        }


        // org principal has no permission on entityset
        val organizationAcl = Acl(
                AclKey(organizationID),
                setOf(Ace(user1, EnumSet.of(Permission.OWNER), OffsetDateTime.MAX))
        )
        permissionsApi.updateAcl(AclData(organizationAcl, Action.ADD))

        try {
            loginAs("user1")
            organizationsApi.assembleEntitySets(organizationID, mapOf(es.id to 100))
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(
                    e.undeclaredThrowable.message!!.contains(
                            "EntitySet [${es.id}] is not accessible by organization principal", true
                    )
            )
        } finally {
            loginAs("admin")
        }


        // org principal has only permission on entityset
        val materializePermissions = EnumSet.of(Permission.MATERIALIZE)
        val esMaterializationAcl = Acl(
                AclKey(es.id),
                setOf(Ace(organization.principal, materializePermissions, OffsetDateTime.MAX))
        )
        permissionsApi.updateAcl(AclData(esMaterializationAcl, Action.ADD))

        loginAs("user1")
        organizationsApi.assembleEntitySets(organizationID, mapOf(es.id to 100))


        loginAs("admin")

        // org principal has permission on 1 property
        val propertyTypeId = et.properties.random()
        val propertyType = edmApi.getPropertyType(propertyTypeId)
        val ptMaterializationAcl = Acl(
                AclKey(es.id, propertyTypeId),
                setOf(Ace(organization.principal, materializePermissions, OffsetDateTime.MAX))
        )
        permissionsApi.updateAcl(AclData(ptMaterializationAcl, Action.ADD))

        loginAs("user1")
        organizationsApi.assembleEntitySets(organizationID, mapOf(es.id to 100))

        val organizationDataSource = TestAssemblerConnectionManager.connect(organizationID)
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es))
                Assert.assertEquals(PostgresColumn.ENTITY_SET_ID.name, rs.metaData.getColumnName(1))
                Assert.assertEquals(PostgresColumn.ID.name, rs.metaData.getColumnName(2))
                Assert.assertEquals(
                        ResultSetAdapters.mapMetadataOptionToPostgresColumn(MetadataOption.ENTITY_KEY_IDS),
                        rs.metaData.getColumnName(3)
                )
                Assert.assertEquals(propertyType.type.fullQualifiedNameAsString, rs.metaData.getColumnName(4))
            }
        }


        // org principal has permission on all properties
        loginAs("admin")
        et.properties.forEach {
            val acl = Acl(
                    AclKey(es.id, it),
                    setOf(Ace(organization.principal, materializePermissions, OffsetDateTime.MAX))
            )
            permissionsApi.updateAcl(AclData(acl, Action.ADD))
        }

        loginAs("user1")
        organizationsApi.assembleEntitySets(organizationID, mapOf(es.id to 100))

        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es))
                Assert.assertEquals(PostgresColumn.ENTITY_SET_ID.name, rs.metaData.getColumnName(1))
                Assert.assertEquals(PostgresColumn.ID.name, rs.metaData.getColumnName(2))
                Assert.assertEquals(
                        ResultSetAdapters.mapMetadataOptionToPostgresColumn(MetadataOption.ENTITY_KEY_IDS),
                        rs.metaData.getColumnName(3)
                )
                val columnNames = TestAssemblerConnectionManager.getColumnNames(rs)
                et.properties.forEach {
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
                try {
                    stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es))
                    Assert.fail("Should have thrown Exception but did not!")
                } catch (e: PSQLException) {
                    Assert.assertTrue(
                            e.message!!
                                    .contains("permission denied for materialized view ${es.name}", true)
                    )
                }
            }
        }


        // add read for user1 on entityset, but no properties and re-materialize
        loginAs("admin")

        val readPermissions = EnumSet.of(Permission.READ)
        val esReadAcl = Acl(AclKey(es.id), setOf(Ace(user1, readPermissions, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(esReadAcl, Action.ADD))

        organizationsApi.assembleEntitySets(organizationID, mapOf(es.id to 100))

        // try to select all columns
        user1OrganizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                try {
                    stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es))
                    Assert.fail("Should have thrown Exception but did not!")
                } catch (e: PSQLException) {
                    Assert.assertTrue(
                            e.message!!
                                    .contains("permission denied for materialized view ${es.name}", true)
                    )
                }
            }
        }

        // try to select only columns, which the user is authorized for
        user1OrganizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                //  user1 can access edges, since it has permission to 1 entityset
                stmt.executeQuery(TestAssemblerConnectionManager.selectEdgesOfEntitySetsSql())
                val rs = stmt.executeQuery(
                        TestAssemblerConnectionManager
                                .selectFromEntitySetSql(
                                        es,
                                        setOf(
                                                PostgresColumn.ENTITY_SET_ID.name,
                                                PostgresColumn.ID.name,
                                                ResultSetAdapters.mapMetadataOptionToPostgresColumn(
                                                        MetadataOption.ENTITY_KEY_IDS
                                                )
                                        )
                                )
                )
                Assert.assertEquals(PostgresColumn.ENTITY_SET_ID.name, rs.metaData.getColumnName(1))
                Assert.assertEquals(PostgresColumn.ID.name, rs.metaData.getColumnName(2))
                Assert.assertEquals(
                        ResultSetAdapters.mapMetadataOptionToPostgresColumn(MetadataOption.ENTITY_KEY_IDS),
                        rs.metaData.getColumnName(3)
                )
            }
        }


        // add read for user1 on 1 property and re-materialize
        val ptReadAcl = Acl(AclKey(es.id, propertyTypeId), setOf(Ace(user1, readPermissions, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(ptReadAcl, Action.ADD))

        organizationsApi.assembleEntitySets(organizationID, mapOf(es.id to 100))

        // try to select all columns
        user1OrganizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                try {
                    stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es))
                    Assert.fail("Should have thrown Exception but did not!")
                } catch (e: PSQLException) {
                    Assert.assertTrue(
                            e.message!!
                                    .contains("permission denied for materialized view ${es.name}", true)
                    )
                }
            }
        }

        // try to select only columns, which the user is authorized for
        user1OrganizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(
                        TestAssemblerConnectionManager
                                .selectFromEntitySetSql(
                                        es, setOf(
                                        PostgresColumn.ENTITY_SET_ID.name,
                                        PostgresColumn.ID.name,
                                        ResultSetAdapters.mapMetadataOptionToPostgresColumn(
                                                MetadataOption.ENTITY_KEY_IDS
                                        ),
                                        propertyType.type.fullQualifiedNameAsString
                                )
                                )
                )
                Assert.assertEquals(PostgresColumn.ENTITY_SET_ID.name, rs.metaData.getColumnName(1))
                Assert.assertEquals(PostgresColumn.ID.name, rs.metaData.getColumnName(2))
                Assert.assertEquals(
                        ResultSetAdapters.mapMetadataOptionToPostgresColumn(MetadataOption.ENTITY_KEY_IDS),
                        rs.metaData.getColumnName(3)
                )
                Assert.assertEquals(propertyType.type.fullQualifiedNameAsString, rs.metaData.getColumnName(4))
            }
        }


        // add read for user1 on all properties and re-materialize
        et.properties.forEach {
            val acl = Acl(AclKey(es.id, it), setOf(Ace(user1, readPermissions, OffsetDateTime.MAX)))
            permissionsApi.updateAcl(AclData(acl, Action.ADD))
        }

        organizationsApi.assembleEntitySets(organizationID, mapOf(es.id to 100))

        user1OrganizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es))
                Assert.assertEquals(PostgresColumn.ENTITY_SET_ID.name, rs.metaData.getColumnName(1))
                Assert.assertEquals(PostgresColumn.ID.name, rs.metaData.getColumnName(2))
                Assert.assertEquals(
                        ResultSetAdapters.mapMetadataOptionToPostgresColumn(MetadataOption.ENTITY_KEY_IDS),
                        rs.metaData.getColumnName(3)
                )
                val columns = TestAssemblerConnectionManager.getColumnNames(rs)
                et.properties.forEach {
                    Assert.assertTrue(columns.contains(edmApi.getPropertyType(it).type.fullQualifiedNameAsString))
                }
            }
        }

        loginAs("admin")
    }

    @Test
    fun testRefreshRate() {
        val et = createEntityType()
        val es = createEntitySet(et)
        val propertyFqns = entitySetsApi.getPropertyTypesForEntitySet(es.id)
                .map { it.key to it.value.type.fullQualifiedNameAsString }.toMap()
        // grant materialize to org principal
        grantMaterializePermissions(organization, es, et.properties)

        // try to add < 1 refresh rates
        try {
            organizationsApi.assembleEntitySets(organizationID, mapOf(es.id to 0))
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(
                    e.undeclaredThrowable.message!!.contains(
                            "Minimum refresh rate is 1 minute.", true
                    )
            )
        }

        try {
            organizationsApi.assembleEntitySets(organizationID, mapOf(es.id to -12))
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(
                    e.undeclaredThrowable.message!!.contains(
                            "Minimum refresh rate is 1 minute.", true
                    )
            )
        }

        // materialize with 2 min refresh rate
        var refreshRate = 2
        organizationsApi.assembleEntitySets(organizationID, mapOf(es.id to refreshRate))

        // add data
        Assert.assertFalse(
                organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )
        val testData = TestDataFactory.randomStringEntityData(numberOfEntities, et.properties).values.toList()
        val ids = dataApi.createEntities(es.id, testData)
        val testDataWithIds = ids.zip(testData).toMap()

        Assert.assertTrue(
                organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                        .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
        )

        // data is not supposed to be there, only the columns until automatic refresh
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es))
                // all columns are there
                (1..rs.metaData.columnCount).forEach {
                    val columnName = rs.metaData.getColumnName(it)
                    if (columnName != PostgresColumn.ID.name && columnName != PostgresColumn.ENTITY_SET_ID.name
                            && columnName != ResultSetAdapters
                                    .mapMetadataOptionToPostgresColumn(MetadataOption.ENTITY_KEY_IDS)) {
                        Assert.assertTrue(propertyFqns.values.contains(columnName))
                    }
                }
                // no data is there yet
                Assert.assertFalse(rs.next())
            }
        }

        // wait until automatic refresh
        Thread.sleep(refreshRate.toLong() * 60 * 1000)


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
                                getStringResult(rs, fqn)
                        )
                    }
                    index++

                } while (rs.next())

                Assert.assertEquals(numberOfEntities, index)
            }
        }


        // set refresh rate to 1 min
        refreshRate = 1
        organizationsApi.updateRefreshRate(organizationID, es.id, refreshRate)

        // delete all data
        dataApi.deleteAllEntitiesFromEntitySet(es.id, DeleteType.Hard)

        // wait until automatic refresh
        Thread.sleep(refreshRate.toLong() * 60 * 1000)

        // data is not supposed to be there, only the columns
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es))
                // all columns are there
                (1..rs.metaData.columnCount).forEach {
                    val columnName = rs.metaData.getColumnName(it)
                    if (columnName != PostgresColumn.ID.name && columnName != PostgresColumn.ENTITY_SET_ID.name
                            && columnName != ResultSetAdapters
                                    .mapMetadataOptionToPostgresColumn(MetadataOption.ENTITY_KEY_IDS)) {
                        Assert.assertTrue(propertyFqns.values.contains(columnName))
                    }
                }
                // no data is there
                Assert.assertFalse(rs.next())
            }
        }


        // set refresh rate to null
        organizationsApi.deleteRefreshRate(organizationID, es.id)

        // add data again
        dataApi.createEntities(es.id, testData)

        // wait until last value of automatic refresh
        Thread.sleep(refreshRate.toLong() * 60 * 1000)

        // data is still not supposed to be there, only the columns
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(es))
                // all columns are there
                (1..rs.metaData.columnCount).forEach {
                    val columnName = rs.metaData.getColumnName(it)
                    if (columnName != PostgresColumn.ID.name && columnName != PostgresColumn.ENTITY_SET_ID.name
                            && columnName != ResultSetAdapters
                                    .mapMetadataOptionToPostgresColumn(MetadataOption.ENTITY_KEY_IDS)) {
                        Assert.assertTrue(propertyFqns.values.contains(columnName))
                    }
                }
                // no data is there yet
                Assert.assertFalse(rs.next())
            }
        }
    }


    /**
     * Add permission to materialize entity set and it's properties to organization principal
     */
    private fun grantMaterializePermissions(organization: Organization, entitySet: EntitySet, properties: Set<UUID>) {
        val newPermissions = EnumSet.of(Permission.MATERIALIZE)
        val entitySetAcl = Acl(
                AclKey(entitySet.id),
                setOf(Ace(organization.principal, newPermissions, OffsetDateTime.MAX))
        )
        permissionsApi.updateAcl(AclData(entitySetAcl, Action.ADD))

        // add permissions on properties
        properties.forEach {
            val propertyTypeAcl = Acl(
                    AclKey(entitySet.id, it),
                    setOf(Ace(organization.principal, newPermissions, OffsetDateTime.MAX))
            )
            permissionsApi.updateAcl(AclData(propertyTypeAcl, Action.ADD))
        }
    }

    private fun getStringResult(rs: ResultSet, column: String): String {
        return PostgresArrays.getTextArray(rs, column)[0]
    }
}