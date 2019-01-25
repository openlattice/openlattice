package com.openlattice.rehearsal.search

import com.google.common.collect.HashMultimap
import com.google.common.collect.Lists
import com.google.common.collect.SetMultimap
import com.openlattice.data.UpdateType
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.rehearsal.SetupTestData
import com.openlattice.rehearsal.edm.PERSON_NAME
import com.openlattice.rehearsal.edm.PERSON_NAMESPACE
import com.openlattice.search.requests.DataSearchResult
import com.openlattice.search.requests.Search
import com.openlattice.search.requests.SearchConstraints
import com.openlattice.search.requests.SearchDetails
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
                } catch (e: UndeclaredThrowableException) {
                }
            }
        }
    }

    @Test
    fun testSimpleSearchOnLinkedEntities() {
        val socratesAId = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val socratesBId = edmApi.getEntitySetId(importedEntitySets.keys.last())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId, socratesBId))

        Thread.sleep(60000L) // wait for indexing to finish

        val simpleSearchConstraint1 = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")

        val result1 = searchApi.searchEntitySetData(simpleSearchConstraint1)
        Assert.assertTrue(result1.numHits > 0)
        Assert.assertTrue(result1.hits.map {
            it[FullQualifiedName("nc.PersonGivenName")]
        }.contains(setOf("Beverly")))

        val simpleSearchConstraint2 = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "Fermin")
        val result2 = searchApi.searchEntitySetData(simpleSearchConstraint2)

        Assert.assertTrue(result2.numHits > 0)
        Assert.assertTrue(result2.hits.flatMap {
            it[FullQualifiedName("nc.PersonGivenName")]
        }.contains("Fermin"))

        edmApi.deleteEntitySet(esLinked.id)
    }

    @Test
    fun testAdvancedSearchOnLinkedEntities() {
        val socratesAId = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val socratesBId = edmApi.getEntitySetId(importedEntitySets.keys.last())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId, socratesBId))

        Thread.sleep(60000L) // wait for indexing to finish

        val lastNameId = edmApi.getPropertyTypeId("nc", "PersonSurName")
        val advancedSearchConstraint1 = SearchConstraints.advancedSearchConstraints(
                arrayOf(esLinked.id),
                0,
                100,
                listOf(SearchDetails("*", lastNameId, false)))
        val result1 = searchApi.searchEntitySetData(advancedSearchConstraint1)

        Assert.assertTrue(result1.numHits > 0)
        Assert.assertTrue(result1.hits.map { it[FullQualifiedName("nc.PersonSurName")] }
                .contains(setOf("Qwe")))

        val advancedSearchConstraint2 = SearchConstraints.advancedSearchConstraints(
                arrayOf(esLinked.id),
                0,
                100,
                listOf(SearchDetails("Morris", lastNameId, false)))
        val result2 = searchApi.searchEntitySetData(advancedSearchConstraint2)

        Assert.assertTrue(result2.numHits > 0)
        Assert.assertTrue(result2.hits.flatMap { it[FullQualifiedName("nc.PersonSurName")] }
                .contains("Morris"))

        edmApi.deleteEntitySet(esLinked.id)
    }

    @Test
    fun testCreateEmptyLinkingEntitySet() {
        val esLinked = createEntitySet(personEt, true, setOf())
        Thread.sleep(60000L) // wait for indexing to finish

        val simpleSearchConstraint = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val result = searchApi.searchEntitySetData(simpleSearchConstraint)
        Assert.assertEquals(DataSearchResult(0, Lists.newArrayList()), result)

        edmApi.deleteEntitySet(esLinked.id)
    }

    @Test
    fun testDeleteLinkingEntitySet() {
        val socratesAId = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId))
        Thread.sleep(60000L) // wait for indexing to finish

        val simpleSearchConstraint = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val result = searchApi.searchEntitySetData(simpleSearchConstraint)

        edmApi.deleteEntitySet(esLinked.id)
        Thread.sleep(60000L) // wait for indexing to finish

        Assert.assertEquals(
                DataSearchResult(0, Lists.newArrayList()),
                searchApi.searchEntitySetData(simpleSearchConstraint))
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

        Thread.sleep(60000L) // wait for indexing to finish

        val simpleSearchConstraints1 = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked1.id), 0, 100, "*")
        val result1 = searchApi.searchEntitySetData(simpleSearchConstraints1)
        val simpleSearchConstraints2 = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked2.id), 0, 100, "*")
        val result2 = searchApi.searchEntitySetData(simpleSearchConstraints2)

        Assert.assertTrue(result1.numHits > 0)
        Assert.assertEquals(result1.numHits, result2.numHits)

        edmApi.deleteEntitySet(esLinked1.id)
        edmApi.deleteEntitySet(esLinked2.id)
    }

    @Test
    fun testDeleteLinkedEntitySet() {
        val socratesAId = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val socratesBId = edmApi.getEntitySetId(importedEntitySets.keys.last())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId, socratesBId))

        Thread.sleep(60000L) // wait for indexing to finish

        val simpleSearchConstraint = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val resultsAB = searchApi.searchEntitySetData(simpleSearchConstraint)

        edmApi.deleteEntitySet(socratesBId)
        val resultsA = searchApi.searchEntitySetData(simpleSearchConstraint)

        Assert.assertTrue(resultsAB.numHits > resultsA.numHits)
        Assert.assertTrue(resultsAB.hits.map { it[FullQualifiedName("nc.PersonSurName")] }
                .contains(setOf("Qwe")))
        Assert.assertFalse(resultsA.hits.map { it[FullQualifiedName("nc.PersonSurName")] }
                .contains(setOf("Qwe")))

        //redo import
        importDataSet(importedEntitySets.values.last().first, importedEntitySets.values.last().second)
        Thread.sleep(10000L)
        while (!checkLinkingFinished(importedEntitySets.keys)) {
            Thread.sleep(5000L)
        }

        edmApi.deleteEntitySet(esLinked.id)
    }

    @Test
    fun testAddAndRemoveLinkedEntitySet() {
        val socratesAId = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val socratesBId = edmApi.getEntitySetId(importedEntitySets.keys.last())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId))

        Thread.sleep(60000L) // wait for indexing to finish

        val simpleSearchConstraint = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val resultsA1 = searchApi.searchEntitySetData(simpleSearchConstraint)

        linkingApi.addEntitySetsToLinkingEntitySet(esLinked.id, setOf(socratesBId))
        Thread.sleep(60000L) // wait for indexing
        val resultsAB = searchApi.searchEntitySetData(simpleSearchConstraint)

        Assert.assertTrue(resultsAB.hits.flatMap { it[FullQualifiedName("nc.PersonSurName")] }.toSet()
                .containsAll(resultsA1.hits.flatMap { it[FullQualifiedName("nc.PersonSurName")] }.toSet()))
        Assert.assertTrue(resultsAB.hits.flatMap { it[FullQualifiedName("nc.PersonSurName")] }
                .contains("Qwe"))

        linkingApi.removeEntitySetsFromLinkingEntitySet(esLinked.id, setOf(socratesBId))
        Thread.sleep(60000L) // wait for indexing

        val resultsA2 = searchApi.searchEntitySetData(simpleSearchConstraint)
        Assert.assertEquals(resultsA1, resultsA2)

        edmApi.deleteEntitySet(esLinked.id)
    }

    @Test
    fun testAddPropertyType() {
        val socratesAId = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId))

        Thread.sleep(60000L) // wait for indexing to finish

        val search = Search(Optional.of(esLinked.name), Optional.of(personEt.id), Optional.empty(), 0, 1)
        val properties1 = searchApi.executeEntitySetKeywordQuery(search).hits[0]["propertyTypes"] as ArrayList<LinkedHashMap<String, Any>>

        val newPropertyType = createPropertyType()
        edmApi.addPropertyTypeToEntityType(personEt.id, newPropertyType.id)

        Thread.sleep(60000L) // wait for indexing to finish

        val properties2 = searchApi.executeEntitySetKeywordQuery(search).hits[0]["propertyTypes"] as ArrayList<LinkedHashMap<String, Any>>

        Assert.assertEquals(properties1.size + 1, properties2.size)
        Assert.assertTrue(properties2.containsAll(properties1))
        val newPropertyHit = properties2.find { it["id"] == newPropertyType.id.toString() }!!
        Assert.assertEquals(newPropertyType.title, newPropertyHit["title"])
        Assert.assertEquals(newPropertyType.description, newPropertyHit["description"])

        edmApi.deleteEntitySet(esLinked.id)
    }

    @Test
    fun testCreateUpdateDeleteEntities() {
        val socratesAId = edmApi.getEntitySetId(importedEntitySets.keys.first())
        val socratesBId = edmApi.getEntitySetId(importedEntitySets.keys.last())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId, socratesBId))

        Thread.sleep(60000L) // wait for indexing to finish
        val simpleSearchConstraint = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val result1 = searchApi.searchEntitySetData(simpleSearchConstraint)
        logger.info(result1.hits.toString())

        // Create
        val testDataProperties = setOf(
                UUID.fromString("d0935a7e-efd3-4903-b673-0869ef527dea"), // middle name
                UUID.fromString("e9a0b4dc-5298-47c1-8837-20af172379a5"), // given name
                UUID.fromString("7b038634-a0b4-4ce1-a04f-85d1775937aa")) // surname
        val entityData = testDataProperties.map { it to setOf("test") }.toMap()
        val newAEntityIds = dataApi.createEntities(socratesAId, listOf(entityData))

        Thread.sleep(10000L) // wait for linking to finish
        while (!checkLinkingFinished(importedEntitySets.keys)) {
            Thread.sleep(5000L)
        }
        Thread.sleep(60000L) // wait for indexing to finish

        val result2 = searchApi.searchEntitySetData(simpleSearchConstraint)
        logger.info(result2.hits.toString())

        Assert.assertTrue(result2.hits
                .map { it[FullQualifiedName("nc.PersonGivenName")] }
                .any { it.contains("test") })


        // Update
        dataApi.updateEntitiesInEntitySet(
                socratesAId,
                mapOf(newAEntityIds.first() to entityData),
                UpdateType.Replace)

        Thread.sleep(10000L) // wait for linking to finish
        while (!checkLinkingFinished(importedEntitySets.keys)) {
            Thread.sleep(5000L)
        }
        Thread.sleep(60000L) // wait for indexing to finish

        val result3 = searchApi.searchEntitySetData(simpleSearchConstraint)
        logger.info(result3.hits.toString())

        Assert.assertEquals(result2.numHits, result3.numHits)
        Assert.assertTrue(result3.hits
                .map { it[FullQualifiedName("nc.PersonGivenName")] }
                .any { it.contains("newtest") })
        Assert.assertTrue(result3.hits
                .map { it[FullQualifiedName("nc.PersonGivenName")] }
                .none { it.contains("test") })

        // Delete:
        // when deleting entity, but there are still entities with that linking_id
        // when deleting entity, and there is no left entity left with that linking_id
        val entityData2 = testDataProperties.map { it to setOf("newtestt") }.toMap()
        // Note: we assume, when creating 2 entities with same values, that they get linked in this tests
        val newBEntityIds = dataApi.createEntities(socratesBId, listOf(entityData2, entityData2))

        Thread.sleep(10000L) // wait for linking to finish
        while (!checkLinkingFinished(importedEntitySets.keys)) {
            Thread.sleep(5000L)
        }
        Thread.sleep(60000L) // wait for indexing to finish

        val result4 = searchApi.searchEntitySetData(simpleSearchConstraint)
        logger.info(result4.hits.toString())
        val testDataGotLinked = (result4.numHits == result3.numHits) // 1 entity got created

        if (testDataGotLinked) {
            Assert.assertTrue(result4.hits.any {
                it[FullQualifiedName("nc.PersonGivenName")] == setOf("newtest", "newtestt")
            })
        } else {
            Assert.assertTrue(result4.hits.any {
                it[FullQualifiedName("nc.PersonGivenName")] == setOf("newtest")
            })
            Assert.assertTrue(result4.hits.any {
                it[FullQualifiedName("nc.PersonGivenName")] == setOf("newtestt")
            })
        }

        dataApi.clearEntityFromEntitySet(socratesBId, newBEntityIds.first()) // delete first entity with value newtestt

        Thread.sleep(10000L) // wait for linking to finish
        while (!checkLinkingFinished(importedEntitySets.keys)) {
            Thread.sleep(5000L)
        }
        Thread.sleep(60000L) // wait for indexing and linking to finish

        val result5 = searchApi.searchEntitySetData(simpleSearchConstraint)
        logger.info(result5.hits.toString())
        if (testDataGotLinked) {
            Assert.assertTrue(result5.hits.any {
                it[FullQualifiedName("nc.PersonGivenName")] == setOf("newtest", "newtestt")
            })
        } else {
            Assert.assertTrue(result5.hits.any {
                it[FullQualifiedName("nc.PersonGivenName")] == setOf("newtest")
            })
            Assert.assertTrue(result5.hits.any {
                it[FullQualifiedName("nc.PersonGivenName")] == setOf("newtestt")
            })
        }

        dataApi.clearEntityFromEntitySet(socratesBId, newBEntityIds.last()) // delete last entity with value newtestt
        Thread.sleep(60000L) // wait for indexing to finish, we don't need to wait for linking here

        val result6 = searchApi.searchEntitySetData(simpleSearchConstraint)
        logger.info(result6.hits.toString())
        Assert.assertTrue(result6.hits.none {
            it[FullQualifiedName("nc.PersonGivenName")].contains("newtestt")
        })
        Assert.assertTrue(result6.hits.any {
            it[FullQualifiedName("nc.PersonGivenName")] == setOf("newtest")
        })

        edmApi.deleteEntitySet(esLinked.id)
    }
}