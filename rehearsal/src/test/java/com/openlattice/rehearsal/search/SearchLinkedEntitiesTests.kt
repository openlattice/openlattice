package com.openlattice.rehearsal.search

import com.openlattice.rehearsal.SetupTestData
import com.openlattice.rehearsal.edm.PERSON_NAME
import com.openlattice.rehearsal.edm.PERSON_NAMESPACE
import com.openlattice.search.requests.SearchConstraints
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.LoggerFactory

class SearchLinkedEntitiesTests : SetupTestData() {
    companion object {
        val logger = LoggerFactory.getLogger(SearchLinkedEntitiesTests::class.java)

        @JvmStatic
        @BeforeClass
        fun init() {
            importDataSet("socratesA.yaml", "test_linked_ppl_1.csv")
            importDataSet("socratesB.yaml", "test_linked_ppl_2.csv")

            Thread.sleep(5000)
            while (!checkLinkingFinished(setOf("SocratesTestA", "SocratesTestB"))) {
                Thread.sleep(3000)
            }
        }
    }

    @Test
    fun testSimpleSearchOnLinkedEntities() {
        val personEntityTypeId = edmApi.getEntityTypeId(PERSON_NAMESPACE, PERSON_NAME)
        val personEt = edmApi.getEntityType(personEntityTypeId)

        val socratesAId = edmApi.getEntitySetId("SocratesTestA")
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId))

        val simpleSearchConstraints = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val result = searchApi.searchEntitySetData(simpleSearchConstraints)

        Assert.assertEquals(2L, result.numHits)
        Assert.assertTrue(result.hits.map { it[FullQualifiedName("nc.PersonGivenName")] }.contains(setOf("Beverlye")))

        edmApi.deleteEntitySet(esLinked.id)
    }
}