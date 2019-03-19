package com.openlattice.rehearsal.organization

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.data.DataEdgeKey
import com.openlattice.data.EntityDataKey
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.Organization
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import org.apache.commons.lang.RandomStringUtils
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.util.UUID
import java.util.EnumSet
import java.util.Optional

private const val numberOfEntities = 10

class AssemblerTest : MultipleAuthenticatedUsersBase() {
    companion object {
        private lateinit var organizationID: UUID
        private lateinit var organizationName: String

        @JvmStatic
        @BeforeClass
        fun init() {
            loginAs("admin")

            organizationName = RandomStringUtils.randomAlphanumeric(5)
            val organization = Organization(
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
        val src = MultipleAuthenticatedUsersBase.createEntityType()
        val dst = MultipleAuthenticatedUsersBase.createEntityType()
        val edge = MultipleAuthenticatedUsersBase.createEdgeEntityType()

        val esSrc = MultipleAuthenticatedUsersBase.createEntitySet(src)
        val esDst = MultipleAuthenticatedUsersBase.createEntitySet(dst)
        val esEdge = MultipleAuthenticatedUsersBase.createEntitySet(edge)

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

        // materialize src entity set
        organizationsApi.assembleEntitySets(organizationID, setOf(esSrc.id))

        Assert.assertEquals(
                setOf(esSrc.id),
                organizationsApi
                        .getOrganizationEntitySets(organizationID, EnumSet.of(OrganizationEntitySetFlag.MATERIALIZED))
                        .keys)
    }
}