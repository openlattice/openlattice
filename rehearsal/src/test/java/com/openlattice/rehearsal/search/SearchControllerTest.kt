package com.openlattice.rehearsal.search

import com.google.common.base.Strings
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.rehearsal.SetupTestData
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import com.openlattice.search.requests.SearchTerm
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.LoggerFactory
import java.util.*

class SearchControllerTest : MultipleAuthenticatedUsersBase() {
    companion object {
        val logger = LoggerFactory.getLogger(SearchControllerTest::class.java)

        @JvmStatic
        @BeforeClass
        fun init() {
            loginAs("admin")
        }
    }

    @Test
    fun testOrganizationIndex() {
        val organization = TestDataFactory.organization()
        val id = organizationsApi.createOrganizationIfNotExists(organization)
        Thread.sleep(3000)

        val search1 = searchApi.executeOrganizationSearch(
                SearchTerm(organization.title, 0, 10, Optional.empty<Boolean>()))
        Assert.assertTrue(search1.hits.mapTo(hashSetOf()) { it["id"] }.contains(id.toString()))

        val newDescription = Strings.repeat(organization.description, 2)
        organizationsApi.updateDescription(id, newDescription)
        Thread.sleep(3000)

        val search2 = searchApi.executeOrganizationSearch(
                SearchTerm(newDescription, 0, 10, Optional.empty<Boolean>()))
        Assert.assertTrue(search2.hits.mapTo(hashSetOf()) { it["id"] }.contains(id.toString()))

        searchApi.triggerOrganizationIndex(id)
        searchApi.triggerAllOrganizationsIndex()

        organizationsApi.destroyOrganization(id)
        val search3 = searchApi.executeOrganizationSearch(
                SearchTerm(organization.title, 0, 10, Optional.empty<Boolean>()))
        Assert.assertEquals(0, search3.hits.size)
    }

}