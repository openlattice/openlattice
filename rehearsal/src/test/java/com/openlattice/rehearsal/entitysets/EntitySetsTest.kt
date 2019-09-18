package com.openlattice.rehearsal.entitysets

import com.google.common.collect.ImmutableList
import com.openlattice.data.DataExpiration
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.edm.set.ExpirationBase
import com.openlattice.graph.query.GraphQueryState
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.rehearsal.assertException
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import com.openlattice.rehearsal.edm.EdmTestConstants
import com.openlattice.rehearsal.organization.OrganizationControllerCallHelper
import com.openlattice.rehearsal.organization.OrganizationsControllerTest
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.lang.reflect.UndeclaredThrowableException
import java.util.*

class EntitySetsTest : MultipleAuthenticatedUsersBase() {

    companion object {
        @JvmStatic
        @BeforeClass
        fun init() {
            loginAs("admin")
        }
    }

    @Test
    fun testAddAndRemoveLinkedEntitySets() {
        val pt = createPropertyType()
        val et = createEntityType(pt.id)
        val linkingEs = createEntitySet(et, true, setOf())

        val es = createEntitySet(EdmTestConstants.personEt)

        entitySetsApi.addEntitySetsToLinkingEntitySet(linkingEs.id, setOf<UUID>(es.id))
        Assert.assertEquals(es.id, entitySetsApi.getEntitySet(linkingEs.id).linkedEntitySets.single())

        entitySetsApi.removeEntitySetsFromLinkingEntitySet(linkingEs.id, setOf(es.id))
        Assert.assertEquals(setOf<UUID>(), entitySetsApi.getEntitySet(linkingEs.id).linkedEntitySets)
    }

    @Test
    fun testChecksOnAddAndRemoveLinkedEntitySets() {
        // entity set is not linking
        val pt = createPropertyType()
        val et = createEntityType(pt.id)
        val nonLinkingEs = createEntitySet(et, false, setOf())
        val es = createEntitySet(et)

        assertException(
                { entitySetsApi.addEntitySetsToLinkingEntitySet(nonLinkingEs.id, setOf<UUID>(es.id)) },
                "Can't add linked entity sets to a not linking entity set"
        )

        // add non-person entity set
        val linkingEs = createEntitySet(et, true, setOf())
        assertException(
                { entitySetsApi.addEntitySetsToLinkingEntitySet(linkingEs.id, setOf<UUID>(es.id)) },
                "Linked entity sets are of differing entity types than " +
                        EdmTestConstants.personEt.type.fullQualifiedNameAsString
        )

        // remove empty
        assertException(
                { entitySetsApi.removeEntitySetsFromLinkingEntitySet(linkingEs.id, setOf()) },
                "Linked entity sets is empty"
        )
    }

    @Test
    fun testCreateBadDataExpiration() {
        //set expiration policy with negative duration of time until data expires
        val badTTL = -10L
        assertException( {DataExpiration(badTTL, ExpirationBase.LAST_WRITE)},
                "Time until data expiration must not be negative")

        //set expiration policy without required start date
        val tTL = 10L
        assertException( {DataExpiration(tTL, ExpirationBase.DATE_PROPERTY)},
                "Must provide property type for expiration calculation" )
    }

    @Test
    fun testAddExpirationPolicy() {
        val es = createEntitySet()
        Assert.assertNull(es.expiration) // default entity set has no expiration policy

        //set expiration policy
        val tTL = 10L
        val expirationPolicy = DataExpiration(tTL, ExpirationBase.FIRST_WRITE)
        val update = MetadataUpdate(Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.of(expirationPolicy))
        entitySetsApi.updateEntitySetMetadata(es.id, update)
        val es2 = entitySetsApi.getEntitySet(es.id)
        Assert.assertEquals(es2.expiration.timeToExpiration, tTL)
        Assert.assertEquals(es2.expiration.expirationBase, ExpirationBase.FIRST_WRITE)
        Assert.assertTrue(es2.expiration.startDateProperty.isEmpty)
    }

    @Test
    fun testRemoveExpirationPolicy() {
        val es = createEntitySet()

        //set an expiration policy
        val tTL = 10L
        val expirationPolicy = DataExpiration(tTL, ExpirationBase.LAST_WRITE)
        val update = MetadataUpdate(Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.of(expirationPolicy))
        entitySetsApi.updateEntitySetMetadata(es.id, update)
        val es2 = entitySetsApi.getEntitySet(es.id)

        //remove expiration policy
        entitySetsApi.removeDataExpirationPolicy(es2.id)
        val es3 = entitySetsApi.getEntitySet(es.id)
        Assert.assertNull(es3.expiration)
    }
}