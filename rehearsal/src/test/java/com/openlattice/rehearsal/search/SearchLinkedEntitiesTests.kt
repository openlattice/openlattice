package com.openlattice.rehearsal.search

import com.google.common.collect.HashMultimap
import com.google.common.collect.Lists
import com.google.common.collect.SetMultimap
import com.openlattice.edm.type.EntityType
import com.openlattice.rehearsal.SetupTestData
import com.openlattice.rehearsal.edm.PERSON_NAME
import com.openlattice.rehearsal.edm.PERSON_NAMESPACE
import com.openlattice.search.requests.DataSearchResult
import com.openlattice.search.requests.SearchConstraints
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.LoggerFactory
import java.lang.reflect.UndeclaredThrowableException
import java.util.*

class SearchLinkedEntitiesTests : SetupTestData() {
    companion object {
        private val logger = LoggerFactory.getLogger(SearchLinkedEntitiesTests::class.java)
        private val importedEntitySets = mapOf(
                "SocratesTestA" to Pair("socratesA.yaml", "test_linked_ppl_1.csv"),
                "SocratesTestB" to Pair("socratesB.yaml", "test_linked_ppl_2.csv"))

        lateinit var personEt: EntityType

        @JvmStatic
        @BeforeClass
        fun init() {
            importedEntitySets.forEach {
                importDataSet(it.value.first, it.value.second)
            }

            Thread.sleep(10000L)
            while (!checkLinkingFinished(importedEntitySets.keys)) {
                Thread.sleep(5000L)
            }

            loginAs("admin")
            personEt = edmApi.getEntityType(edmApi.getEntityTypeId(PERSON_NAMESPACE, PERSON_NAME))
        }

        @JvmStatic
        @AfterClass
        fun tearDown() {
            importedEntitySets.keys.forEach {
                try {
                    edmApi.deleteEntitySet(edmApi.getEntitySetId(it))
                } catch (e: UndeclaredThrowableException) {}
            }
        }
    }

    @Test
    fun testSimpleSearchOnLinkedEntities() {
        val socratesAId = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId))

        Thread.sleep(30000L) // wait for indexing to finish

        val simpleSearchConstraints = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val result = searchApi.searchEntitySetData(simpleSearchConstraints)
        edmApi.deleteEntitySet(esLinked.id)


        Assert.assertTrue(result.numHits > 0)
        Assert.assertTrue(result.hits.map { it[FullQualifiedName("nc.PersonGivenName")] }
                .contains(setOf("Beverlye")))
    }

    @Test
    fun testAdvancedSearchOnLinkedEntities() {
        //TODO
    }

    @Test
    fun testCreateEmptyLinkingEntitySet() {
        val esLinked = createEntitySet(personEt, true, setOf())
        Thread.sleep(30000L) // wait for indexing to finish

        val simpleSearchConstraints = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val result = searchApi.searchEntitySetData(simpleSearchConstraints)
        edmApi.deleteEntitySet(esLinked.id)

        Assert.assertEquals(DataSearchResult(0, Lists.newArrayList()), result)
    }

    @Test
    fun testDeleteLinkingEntitySet() {
        val socratesAId = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId))
        Thread.sleep(30000L) // wait for indexing to finish

        val simpleSearchConstraints = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val result = searchApi.searchEntitySetData(simpleSearchConstraints)

        edmApi.deleteEntitySet(esLinked.id)
        Thread.sleep(30000L) // wait for indexing to finish

        Assert.assertEquals(
                DataSearchResult(0, Lists.newArrayList()),
                searchApi.searchEntitySetData(simpleSearchConstraints))
        Assert.assertTrue(result.numHits > 0)
    }

    /**
     * Case when an entityset with linked entities is already linked to a linking entity set
     * and consequently the linking ids have been indexed.
     */
    @Test
    fun testCreateLinkingEntitySetWithExistingLinkedEntitySet() {
        val socratesAId = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val esLinked1 = createEntitySet(personEt, true, setOf(socratesAId))
        val esLinked2 = createEntitySet(personEt, true, setOf(socratesAId))

        Thread.sleep(30000L) // wait for indexing to finish

        val simpleSearchConstraints1 = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked1.id), 0, 100, "*")
        val result1 = searchApi.searchEntitySetData(simpleSearchConstraints1)
        val simpleSearchConstraints2 = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked2.id), 0, 100, "*")
        val result2 = searchApi.searchEntitySetData(simpleSearchConstraints2)

        edmApi.deleteEntitySet(esLinked1.id)
        edmApi.deleteEntitySet(esLinked2.id)

        Assert.assertTrue(result1.numHits > 0)
        Assert.assertEquals(result1.numHits, result2.numHits)
    }

    @Test
    fun testDeleteLinkedEntitySet() {
        val socratesAId = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val socratesBId = edmApi.getEntitySetId(importedEntitySets.keys.last())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId, socratesBId))

        Thread.sleep(30000L) // wait for indexing to finish

        val simpleSearchConstraints = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val numHitsAB = searchApi.searchEntitySetData(simpleSearchConstraints).numHits

        edmApi.deleteEntitySet(socratesBId)
        val numHitsA = searchApi.searchEntitySetData(simpleSearchConstraints).numHits

        //redo import
        importDataSet(importedEntitySets.values.last().first, importedEntitySets.values.last().second)
        Thread.sleep(10000L)
        while (!checkLinkingFinished(importedEntitySets.keys)) {
            Thread.sleep(5000L)
        }

        edmApi.deleteEntitySet(esLinked.id)

        Assert.assertTrue(numHitsAB > numHitsA)
    }

    @Test
    fun testAddAndRemoveLinkedEntitySet() {
        val socratesAId = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val socratesBId = edmApi.getEntitySetId(importedEntitySets.keys.last())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId))

        Thread.sleep(30000L) // wait for indexing to finish

        val simpleSearchConstraints = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val resultsA1 = searchApi.searchEntitySetData(simpleSearchConstraints)

        linkingApi.addEntitySetsToLinkingEntitySet(esLinked.id, setOf(socratesBId))
        Thread.sleep(30000L) // wait for indexing
        val resultsAB = searchApi.searchEntitySetData(simpleSearchConstraints)

        Assert.assertTrue(resultsAB.hits.toSet().containsAll(resultsA1.hits))

        linkingApi.removeEntitySetsFromLinkingEntitySet(esLinked.id, setOf(socratesBId))
        Thread.sleep(30000L) // wait for indexing

        val resultsA2 = searchApi.searchEntitySetData(simpleSearchConstraints)
        edmApi.deleteEntitySet(esLinked.id)

        Assert.assertEquals(resultsA1, resultsA2)
    }

    @Test
    fun testAddPropertyType() {
        val socratesAId = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId))

        Thread.sleep(30000L) // wait for indexing to finish

        val simpleSearchConstraints = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val result1 = searchApi.searchEntitySetData(simpleSearchConstraints)

        val newPropertyType = createPropertyType()
        edmApi.addPropertyTypeToEntityType(personEt.id, newPropertyType.id)
        Thread.sleep(30000L) // wait for indexing to finish
        val result2 = searchApi.searchEntitySetData(simpleSearchConstraints)

        edmApi.deleteEntitySet(esLinked.id)

        Assert.assertEquals(result1.hits.first().keySet().size + 1, result2.hits.first().keySet().size)
        Assert.assertNull(result2.hits.first()[newPropertyType.type])
    }

    @Test
    fun testCreateEntities() {
        val socratesAId = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val socratesBId = edmApi.getEntitySetId(importedEntitySets.keys.last())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId, socratesBId))

        Thread.sleep(30000L) // wait for indexing to finish
        val simpleSearchConstraints = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val result1 = searchApi.searchEntitySetData(simpleSearchConstraints)

        val entityData: SetMultimap<UUID, Any> = HashMultimap.create<UUID, Any>()
        personEt.properties.forEach {
            entityData.put( it, "2018-01-01" ) // TODO
        }
        dataApi.createEntities(socratesAId, listOf(entityData))
        Thread.sleep(60000L) // wait for indexing and linking to finish

        val result2 = searchApi.searchEntitySetData(simpleSearchConstraints)

        if(result1.numHits == result2.numHits) { // data got linked
            //TODO
        } else {
            result2.hits.any { it[FullQualifiedName("nc.PersonGivenName")].first() == "2018-01-01" }
        }
    }

    @Test
    fun testUpdateEntities() {
        //TODO
    }

    @Test
    fun testDeleteEntities() {
        //TODO
    }
}