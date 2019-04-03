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
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import com.openlattice.rehearsal.data.numberOfEntries
import org.apache.commons.lang.RandomStringUtils
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.time.OffsetDateTime
import java.util.*

private const val numberOfEntities = 10

class AssemblerTest : MultipleAuthenticatedUsersBase() {
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
        grantMaterializePermissions(organization, es1, et.properties)
        organizationsApi.synchronizeEdmChanges(organizationID, es1.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es1.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))
        // todo test if new column is there


        val es2 = createEntitySet(et)
        // materialize entity set
        grantMaterializePermissions(organization, es2, setOf())
        organizationsApi.assembleEntitySets(organizationID, setOf(es2.id))


        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es2.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))
        // change property type fqn
        edmApi.updatePropertyTypeMetadata(
                et.properties.first(),
                MetadataUpdate(Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(TestDataFactory.fqn()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[es2.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))
        organizationsApi.synchronizeEdmChanges(organizationID, es1.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es1.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))
        // todo test if column has new name
    }

    @Test
    fun testDataUnsync() {
        val testEntityCount = 20
        val et = createEntityType()
        val es = createEntitySet(et)

        // materialize entity set with all it's properties
        grantMaterializePermissions(organization, es, et.properties)
        organizationsApi.assembleEntitySets(organizationID, setOf(es.id))

        // add data
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        val testData = TestDataFactory.randomStringEntityData(testEntityCount, et.properties).values.toList()
        val ids = dataApi.createEntities(es.id, testData)
        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // refresh
        organizationsApi.refreshDataChanges(organizationID, es.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // todo test if data is in org database


        // update data
        val newTestData = TestDataFactory.randomStringEntityData(20, et.properties).values.toList()
        dataApi.updateEntitiesInEntitySet(es.id, ids.zip(newTestData).toMap(), UpdateType.Replace)
        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // refresh
        organizationsApi.refreshDataChanges(organizationID, es.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // todo test if updated data is in org database

        // delete data
        dataApi.deleteEntities(es.id, ids.subList(0, testEntityCount/2).toSet(), DeleteType.Hard)
        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // refresh
        organizationsApi.refreshDataChanges(organizationID, es.id)
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es.id]!!
                .contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED))
        // todo test if data is deleted org database
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
}