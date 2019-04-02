package com.openlattice.rehearsal.organization

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import com.openlattice.authorization.*
import com.openlattice.data.DataEdgeKey
import com.openlattice.data.EntityDataKey
import com.openlattice.edm.EntitySet
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.Organization
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
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
    fun testEdmUnsyncFlagging() {
        val et = createEntityType()
        val es1 = createEntitySet(et)
        // add permission to materialize entity set to organization principal
        grantMaterializePermissions(organization, es1, setOf())
        // materialize entity set
        organizationsApi.assembleEntitySets(organizationID, setOf(es1.id))

        // add property type
        val pt = createPropertyType()

        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es1.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))
        edmApi.addPropertyTypeToEntityType(et.id, pt.id)
        Assert.assertTrue(organizationsApi.getOrganizationEntitySets(organizationID)[es1.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))


        val es2 = createEntitySet(et)
        // add permission to materialize entity set to organization principal
        grantMaterializePermissions(organization, es2, setOf())
        // materialize entity set
        organizationsApi.assembleEntitySets(organizationID, setOf(es2.id))

        // change property type fqn
        Assert.assertFalse(organizationsApi.getOrganizationEntitySets(organizationID)[es2.id]!!
                .contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED))
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
    }

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