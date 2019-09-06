package com.openlattice.rehearsal.entitysets

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
}