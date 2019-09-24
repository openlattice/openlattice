package com.openlattice.rehearsal.entitysets

import com.openlattice.data.DataExpiration
import com.openlattice.data.DeleteType
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.edm.set.ExpirationBase
import com.openlattice.edm.type.EntityType
import com.openlattice.rehearsal.assertException
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import com.openlattice.rehearsal.edm.EdmTestConstants
import org.apache.commons.lang.RandomStringUtils
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.*

class EntitySetsTest : MultipleAuthenticatedUsersBase() {

    companion object {

        lateinit var personEt: EntityType

        @JvmStatic
        @BeforeClass
        fun init() {
            loginAs("admin")

            personEt = EdmTestConstants.personEt
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
        assertException({ DataExpiration(badTTL, ExpirationBase.LAST_WRITE, DeleteType.Hard) },
                "Time until data expiration must not be negative")

        //set expiration policy without required start date
        val tTL = 10L
        assertException({ DataExpiration(tTL, ExpirationBase.DATE_PROPERTY, DeleteType.Soft) },
                "Must provide property type for expiration calculation")
    }

    @Test
    fun testAddExpirationPolicy() {
        val es = createEntitySet()
        Assert.assertNull(es.expiration) // default entity set has no expiration policy

        //set expiration policy
        val tTL = 10L
        val expirationPolicy = DataExpiration(tTL, ExpirationBase.FIRST_WRITE, DeleteType.Hard)
        val update = MetadataUpdate(Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.of(expirationPolicy))
        entitySetsApi.updateEntitySetMetadata(es.id, update)
        val es2 = entitySetsApi.getEntitySet(es.id)
        Assert.assertEquals(tTL, es2.expiration.timeToExpiration)
        Assert.assertEquals(ExpirationBase.FIRST_WRITE, es2.expiration.expirationBase)
        Assert.assertTrue(es2.expiration.startDateProperty.isEmpty)
    }

    @Test
    fun testRemoveExpirationPolicy() {
        val es = createEntitySet()

        //set an expiration policy
        val tTL = 10L
        val expirationPolicy = DataExpiration(tTL, ExpirationBase.LAST_WRITE, DeleteType.Hard)
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

    @Test
    fun testGetExpiringEntitiesByFirstWrite() {
        //create data to expire
        val es = createEntitySet(personEt)
        val entries = (1..10)
                .map { mapOf(EdmTestConstants.personGivenNameId to setOf(RandomStringUtils.randomAscii(5))) }
        dataApi.createEntities(es.id, entries)

        //set first_write expiration policy
        val tTL: Long = 24 * 60 * 60 * 1000 //one day
        val expirationPolicy = DataExpiration(tTL, ExpirationBase.FIRST_WRITE, DeleteType.Hard)
        val update = MetadataUpdate(Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.of(expirationPolicy))
        entitySetsApi.updateEntitySetMetadata(es.id, update)

        //check expiring entities
        val noExpiringIds = entitySetsApi.getExpiringEntitiesFromEntitySet(es.id, OffsetDateTime.now().toString())
        Assert.assertEquals(0, noExpiringIds.size)
        val expiringIds = entitySetsApi.getExpiringEntitiesFromEntitySet(
                es.id, OffsetDateTime.now().plusDays(1).toString())
        Assert.assertEquals(10, expiringIds.size)
    }

    @Test
    fun testGetExpiringEntitiesByLastWrite() {
        //create data to expire
        val es = createEntitySet(personEt)
        val entries = (1..10)
                .map { mapOf(EdmTestConstants.personGivenNameId to setOf(RandomStringUtils.randomAscii(5))) }
        dataApi.createEntities(es.id, entries)

        //set last_write expiration policy
        val tTL: Long = 24 * 60 * 60 * 1000 //one day
        val expirationPolicy = DataExpiration(tTL, ExpirationBase.LAST_WRITE, DeleteType.Hard)
        val update = MetadataUpdate(Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.of(expirationPolicy))
        entitySetsApi.updateEntitySetMetadata(es.id, update)

        //check expiring entities
        val noExpiringIds = entitySetsApi.getExpiringEntitiesFromEntitySet(es.id, OffsetDateTime.now().toString())
        Assert.assertEquals(0, noExpiringIds.size)
        val expiringIds = entitySetsApi.getExpiringEntitiesFromEntitySet(
                es.id, OffsetDateTime.now().plusDays(1).toString())
        Assert.assertEquals(10, expiringIds.size)
    }

    @Test
    fun testGetExpiringEntitiesByDateProperty() {
        //create entity set
        val es = createEntitySet(personEt)
        val entries = (1..10)
                .map { mapOf(EdmTestConstants.personDateOfBirthId to setOf(LocalDate.now())) }
        dataApi.createEntities(es.id, entries)

        //set date_property expiration policy
        val tTL: Long = 24 * 60 * 60 * 1000 //one day
        val expirationPolicy3 = DataExpiration(tTL, ExpirationBase.DATE_PROPERTY, DeleteType.Hard, Optional.of(EdmTestConstants.personDateOfBirthId))
        val update3 = MetadataUpdate(Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.of(expirationPolicy3))
        entitySetsApi.updateEntitySetMetadata(es.id, update3)

        //check expiring entities
        val noExpiringIds = entitySetsApi.getExpiringEntitiesFromEntitySet(es.id, OffsetDateTime.now().toString())
        Assert.assertEquals(0, noExpiringIds.size)
        val expiringIds = entitySetsApi.getExpiringEntitiesFromEntitySet(
                es.id, OffsetDateTime.now().plusDays(2).toString())
        Assert.assertEquals(10, expiringIds.size)
    }


}