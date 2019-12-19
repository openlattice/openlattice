package com.openlattice.rehearsal.search

import com.google.common.collect.ImmutableList
import com.google.common.collect.Lists
import com.openlattice.authorization.*
import com.openlattice.data.DataEdgeKey
import com.openlattice.data.DeleteType
import com.openlattice.data.EntityDataKey
import com.openlattice.data.UpdateType
import com.openlattice.data.requests.EntitySetSelection
import com.openlattice.data.requests.FileType
import com.openlattice.edm.EdmConstants
import com.openlattice.edm.type.EntityType
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.rehearsal.SetupTestData
import com.openlattice.rehearsal.edm.EdmTestConstants
import com.openlattice.search.requests.*
import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.LoggerFactory
import java.lang.reflect.UndeclaredThrowableException
import java.time.OffsetDateTime
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
            personEt = EdmTestConstants.personEt
        }

        @JvmStatic
        @AfterClass
        fun tearDown() {
            importedEntitySets.keys.forEach {
                try {
                    entitySetsApi.deleteEntitySet(entitySetsApi.getEntitySetId(it))
                } catch (e: UndeclaredThrowableException) {
                }
            }
        }
    }

    @Test
    fun testSimpleSearchOnLinkedEntities() {
        val socratesAId = entitySetsApi.getEntitySetId(importedEntitySets.keys.first())
        val socratesBId = entitySetsApi.getEntitySetId(importedEntitySets.keys.last())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId, socratesBId))

        Thread.sleep(60000L) // wait for indexing to finish

        val simpleSearchConstraint1 = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")

        val result1 = searchApi.searchEntitySetData(simpleSearchConstraint1)
        Assert.assertTrue(result1.numHits > 0)
        Assert.assertTrue(result1.hits.map {
            it[EdmTestConstants.personGivenNameFqn]
        }.contains(setOf("Beverly")))

        val simpleSearchConstraint2 = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "Fermin")
        val result2 = searchApi.searchEntitySetData(simpleSearchConstraint2)

        Assert.assertTrue(result2.numHits > 0)
        Assert.assertTrue(result2.hits.flatMap { it[EdmTestConstants.personGivenNameFqn]!! }.toSet().contains("Fermin"))

        entitySetsApi.deleteEntitySet(esLinked.id)
    }

    @Test
    fun testAdvancedSearchOnLinkedEntities() {
        val socratesAId = entitySetsApi.getEntitySetId(importedEntitySets.keys.first())
        val socratesBId = entitySetsApi.getEntitySetId(importedEntitySets.keys.last())
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
        Assert.assertTrue(result1.hits.map { it[EdmTestConstants.personSurnameFqn] }.contains(setOf("Qwe")))

        val advancedSearchConstraint2 = SearchConstraints.advancedSearchConstraints(
                arrayOf(esLinked.id),
                0,
                100,
                listOf(SearchDetails("Morris", lastNameId, false)))
        val result2 = searchApi.searchEntitySetData(advancedSearchConstraint2)

        Assert.assertTrue(result2.numHits > 0)
        Assert.assertTrue(result2.hits.flatMap { it[EdmTestConstants.personSurnameFqn]!! }.toSet().contains("Morris"))

        entitySetsApi.deleteEntitySet(esLinked.id)
    }

    @Test
    fun testCreateEmptyLinkingEntitySet() {
        val esLinked = createEntitySet(personEt, true, setOf())
        Thread.sleep(60000L) // wait for indexing to finish

        val simpleSearchConstraint = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val result = searchApi.searchEntitySetData(simpleSearchConstraint)
        Assert.assertEquals(DataSearchResult(0, Lists.newArrayList()), result)

        entitySetsApi.deleteEntitySet(esLinked.id)
    }

    @Test
    fun testDeleteLinkingEntitySet() {
        val socratesAId = entitySetsApi.getEntitySetId(importedEntitySets.keys.first())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId))
        Thread.sleep(60000L) // wait for indexing to finish

        val simpleSearchConstraint = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val result = searchApi.searchEntitySetData(simpleSearchConstraint)

        entitySetsApi.deleteEntitySet(esLinked.id)
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
        val socratesAId = entitySetsApi.getEntitySetId(importedEntitySets.keys.first())
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

        entitySetsApi.deleteEntitySet(esLinked1.id)
        entitySetsApi.deleteEntitySet(esLinked2.id)
    }

    @Test
    fun testDeleteLinkedEntitySet() {
        val socratesAId = entitySetsApi.getEntitySetId(importedEntitySets.keys.first())
        val socratesBId = entitySetsApi.getEntitySetId(importedEntitySets.keys.last())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId, socratesBId))

        Thread.sleep(60000L) // wait for indexing to finish

        val simpleSearchConstraint = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val resultsAB = searchApi.searchEntitySetData(simpleSearchConstraint)

        entitySetsApi.deleteEntitySet(socratesBId)
        val resultsA = searchApi.searchEntitySetData(simpleSearchConstraint)

        Assert.assertTrue(resultsAB.numHits > resultsA.numHits)
        Assert.assertTrue(resultsAB.hits.map { it[EdmTestConstants.personSurnameFqn] }.contains(setOf("Qwe")))
        Assert.assertFalse(resultsA.hits.map { it[EdmTestConstants.personSurnameFqn] }.contains(setOf("Qwe")))

        //redo import
        importDataSet(importedEntitySets.values.last().first, importedEntitySets.values.last().second)
        Thread.sleep(10000L)
        while (!checkLinkingFinished(importedEntitySets.keys)) {
            Thread.sleep(5000L)
        }

        entitySetsApi.deleteEntitySet(esLinked.id)
    }

    @Test
    fun testAddAndRemoveLinkedEntitySet() {
        val socratesAId = entitySetsApi.getEntitySetId(importedEntitySets.keys.first())
        val socratesBId = entitySetsApi.getEntitySetId(importedEntitySets.keys.last())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId))

        Thread.sleep(60000L) // wait for indexing to finish

        val simpleSearchConstraint = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val resultsA1 = searchApi.searchEntitySetData(simpleSearchConstraint)

        entitySetsApi.addEntitySetsToLinkingEntitySet(esLinked.id, setOf(socratesBId))
        Thread.sleep(60000L) // wait for indexing
        val resultsAB = searchApi.searchEntitySetData(simpleSearchConstraint)

        Assert.assertTrue(resultsAB.hits.flatMap { it[EdmTestConstants.personSurnameFqn]!! }.toSet()
                .containsAll(resultsA1.hits.flatMap { it[EdmTestConstants.personSurnameFqn]!! }.toSet()))
        Assert.assertTrue(resultsAB.hits.flatMap { it[EdmTestConstants.personSurnameFqn]!! }.contains("Qwe"))

        entitySetsApi.removeEntitySetsFromLinkingEntitySet(esLinked.id, setOf(socratesBId))
        Thread.sleep(60000L) // wait for indexing

        val resultsA2 = searchApi.searchEntitySetData(simpleSearchConstraint)
        Assert.assertEquals(resultsA1, resultsA2)

        entitySetsApi.deleteEntitySet(esLinked.id)
    }

    @Test
    fun testAddPropertyType() {
        val socratesAId = entitySetsApi.getEntitySetId(importedEntitySets.keys.first())
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

        entitySetsApi.deleteEntitySet(esLinked.id)
    }

    @Test
    fun testCreateUpdateDeleteEntities() {
        val socratesAId = entitySetsApi.getEntitySetId(importedEntitySets.keys.first())
        val socratesBId = entitySetsApi.getEntitySetId(importedEntitySets.keys.last())
        val esLinked = createEntitySet(personEt, true, setOf(socratesAId, socratesBId))

        Thread.sleep(60000L) // wait for indexing to finish
        val simpleSearchConstraint = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinked.id), 0, 100, "*")
        val result1 = searchApi.searchEntitySetData(simpleSearchConstraint)
        logger.info(result1.hits.toString())

        // Create
        val testDataProperties = setOf(
                EdmTestConstants.personMiddleNameId,
                EdmTestConstants.personGivenNameId,
                EdmTestConstants.personSurnameId)
        val entityData = testDataProperties.map { it to setOf("test") }.toMap()
        val newAEntityIds = dataApi.createEntities(socratesAId, listOf(entityData))

        Thread.sleep(10000L) // wait for linking to finish
        while (!checkLinkingFinished(importedEntitySets.keys)) {
            Thread.sleep(5000L)
        }
        Thread.sleep(60000L) // wait for indexing to finish

        val result2 = searchApi.searchEntitySetData(simpleSearchConstraint)
        logger.info(result2.hits.toString())

        Assert.assertTrue(result2.hits.map { it[EdmTestConstants.personGivenNameFqn] }.any { it?.contains("test") ?: false })


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
                .map { it[EdmTestConstants.personGivenNameFqn] }
                .any { it?.contains("newtest")  ?: false })
        Assert.assertTrue(result3.hits
                .map { it[EdmTestConstants.personGivenNameFqn] }
                .none { it?.contains("test") ?: true })

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
                it[EdmTestConstants.personGivenNameFqn] == setOf("newtest", "newtestt")
            })
        } else {
            Assert.assertTrue(result4.hits.any { it[EdmTestConstants.personGivenNameFqn] == setOf("newtest") })
            Assert.assertTrue(result4.hits.any { it[EdmTestConstants.personGivenNameFqn] == setOf("newtestt") })
        }

        dataApi.deleteEntity(socratesBId, newBEntityIds.first(), DeleteType.Soft) // delete first entity with value newtestt

        Thread.sleep(10000L) // wait for linking to finish
        while (!checkLinkingFinished(importedEntitySets.keys)) {
            Thread.sleep(5000L)
        }
        Thread.sleep(60000L) // wait for indexing and linking to finish

        val result5 = searchApi.searchEntitySetData(simpleSearchConstraint)
        logger.info(result5.hits.toString())
        if (testDataGotLinked) {
            Assert.assertTrue(result5.hits.any {
                it[EdmTestConstants.personGivenNameFqn] == setOf("newtest", "newtestt")
            })
        } else {
            Assert.assertTrue(result5.hits.any { it[EdmTestConstants.personGivenNameFqn] == setOf("newtest") })
            Assert.assertTrue(result5.hits.any { it[EdmTestConstants.personGivenNameFqn] == setOf("newtestt") })
        }

        dataApi.deleteEntity(socratesBId, newBEntityIds.last(), DeleteType.Soft) // delete last entity with value newtestt
        Thread.sleep(60000L) // wait for indexing to finish, we don't need to wait for linking here

        val result6 = searchApi.searchEntitySetData(simpleSearchConstraint)
        logger.info(result6.hits.toString())
        Assert.assertTrue(result6.hits.none { it[EdmTestConstants.personGivenNameFqn]!!.contains("newtestt") })
        Assert.assertTrue(result6.hits.any { it[EdmTestConstants.personGivenNameFqn] == setOf("newtest") })

        entitySetsApi.deleteEntitySet(esLinked.id)
    }

    @Test
    fun testLinkingEntitySetDataSearchAuthorizations() {
        loginAs("admin")

        importedEntitySets.keys.forEach {
            entitySetsApi.deleteEntitySet(entitySetsApi.getEntitySetId(it))
        }
        Thread.sleep(5000L)

        importedEntitySets.forEach {
            importDataSet(it.value.first, it.value.second)
        }

        Thread.sleep(10000L)
        while (!checkLinkingFinished(importedEntitySets.keys)) {
            Thread.sleep(5000L)
        }

        // remove permissions on imported entity sets
        val esId1 = entitySetsApi.getEntitySetId(importedEntitySets.keys.first())
        val esId2 = entitySetsApi.getEntitySetId(importedEntitySets.keys.last())

        val ess = EntitySetSelection(Optional.of(personEt.properties))
        val esData1 = dataApi.loadEntitySetData(esId1, ess, FileType.json).toList()
        val esData2 = dataApi.loadEntitySetData(esId2, ess, FileType.json).toList()

        val esLinking = createEntitySet(personEt, true, setOf(esId1, esId2))

        logger.info("Testing search authorizations for linking entity set ${esLinking.id}")

        val personPt = EdmTestConstants.personSurnameId
        val personPropertyType = edmApi.getPropertyType(personPt)
        val personPropertiesWithData = setOf(
                EdmTestConstants.personSurnameId,
                EdmTestConstants.personGivenNameId,
                EdmTestConstants.personDateOfBirthId)

        val readPermission = EnumSet.of(Permission.READ)
        esLinking.linkedEntitySets.forEach { esId ->
            permissionsApi.updateAcl(
                    AclData(
                            Acl(AclKey(esId), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))),
                            Action.REMOVE))
            personEt.properties.forEach {
                permissionsApi.updateAcl(
                        AclData(
                                Acl(AclKey(esId, it), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))),
                                Action.REMOVE))
            }
        }

        // create data with admin
        val numberOfEntries = esData1.size + esData2.size
        val ids1 = esData1.map { UUID.fromString(it[EdmConstants.ID_FQN].first() as String) }
        val ids2 = esData2.map { UUID.fromString(it[EdmConstants.ID_FQN].first() as String) }

        // add association to linkingEs
        val at = createEdgeEntityType()
        val et = createEntityType()
        val pt = et.properties.random()
        val propertyType = edmApi.getPropertyType(pt)
        val apt = at.properties.random()
        val associationPropertyType = edmApi.getPropertyType(apt)
        val dst = createEntitySet(et)
        val edge = createEntitySet(at)

        val testDataDst = TestDataFactory.randomStringEntityData(numberOfEntries, et.properties)
        val testDataEdge = TestDataFactory.randomStringEntityData(numberOfEntries, at.properties)

        val entriesDst = ImmutableList.copyOf(testDataDst.values)
        val idsDst = dataApi.createEntities(dst.id, entriesDst)
        val entriesEdge = ImmutableList.copyOf(testDataEdge.values)
        val idsEdge = dataApi.createEntities(edge.id, entriesEdge)


        val edges = (ids1 + ids2).mapIndexed { index, _ ->
            val src = if (index < ids1.size) {
                EntityDataKey(esId1, ids1[index])
            } else {
                EntityDataKey(esId2, ids2[index - ids1.size])
            }
            DataEdgeKey(src, EntityDataKey(dst.id, idsDst[index]), EntityDataKey(edge.id, idsEdge[index]))
        }.toSet()
        dataApi.createEdges(edges)

        val data = ImmutableList.copyOf(dataApi.loadEntitySetData(esLinking.id, ess, FileType.json))
        val linkingIds = data.map { UUID.fromString(it[EdmConstants.ID_FQN].first() as String) }
        val linkingId = linkingIds.random()

        // setup basic search parameters
        val simpleSearchConstraint = SearchConstraints
                .simpleSearchConstraints(arrayOf(esLinking.id), 0, 100, "*")
        val searchTerm = SearchTerm("*", 0, 100)
        val advancedSearchTerm = AdvancedSearch(
                listOf(SearchDetails("*", personPt, false)), 0, 100)
        val neighborsFilter = EntityNeighborsFilter(linkingIds.toSet())


        // try to read data with no permissions on it
        loginAs("user1")

        val noEntitySetData1 = searchApi.searchEntitySetData(simpleSearchConstraint)
        Assert.assertEquals(0L, noEntitySetData1.numHits)
        Assert.assertEquals(0, noEntitySetData1.hits.size)
        val noEntitySetData2 = searchApi.executeEntitySetDataQuery(esLinking.id, searchTerm)
        Assert.assertEquals(0L, noEntitySetData2.numHits)
        Assert.assertEquals(0, noEntitySetData2.hits.size)
        val noEntitySetData3 = searchApi.executeAdvancedEntitySetDataQuery(esLinking.id, advancedSearchTerm)
        Assert.assertEquals(0L, noEntitySetData3.numHits)
        Assert.assertEquals(0, noEntitySetData3.hits.size)

        val noNeighborData1 = searchApi.executeEntityNeighborSearch(esLinking.id, linkingId)
        Assert.assertEquals(0, noNeighborData1.size)
        val noNeighborData2 = searchApi.executeFilteredEntityNeighborSearch(esLinking.id, neighborsFilter)
        Assert.assertEquals(0, noNeighborData2.size)
        val noNeighborData3 = searchApi.executeFilteredEntityNeighborIdsSearch(esLinking.id, neighborsFilter)
        Assert.assertEquals(0, noNeighborData3.size)

        loginAs("admin")


        // try to read data with only permission on linking entityset but no normal entity sets
        val esLinkingReadAcl = Acl(AclKey(esLinking.id), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(esLinkingReadAcl, Action.ADD))

        loginAs("user1")

        val noEntitySetData4 = searchApi.searchEntitySetData(simpleSearchConstraint)
        Assert.assertEquals(0L, noEntitySetData4.numHits)
        Assert.assertEquals(0, noEntitySetData4.hits.size)
        val noEntitySetData5 = searchApi.executeEntitySetDataQuery(esLinking.id, searchTerm)
        Assert.assertEquals(0L, noEntitySetData5.numHits)
        Assert.assertEquals(0, noEntitySetData5.hits.size)
        val noEntitySetData6 = searchApi.executeAdvancedEntitySetDataQuery(esLinking.id, advancedSearchTerm)
        Assert.assertEquals(0L, noEntitySetData6.numHits)
        Assert.assertEquals(0, noEntitySetData6.hits.size)

        val noNeighborData4 = searchApi.executeEntityNeighborSearch(esLinking.id, linkingId)
        Assert.assertEquals(0, noNeighborData4.size)
        val noNeighborData5 = searchApi.executeFilteredEntityNeighborSearch(esLinking.id, neighborsFilter)
        Assert.assertEquals(0, noNeighborData5.size)
        val noNeighborData6 = searchApi.executeFilteredEntityNeighborIdsSearch(esLinking.id, neighborsFilter)
        Assert.assertEquals(0, noNeighborData6.size)

        loginAs("admin")


        // try to read data with only permission on 1 normal entityset but not the other
        val es1ReadAcl = Acl(AclKey(esId1), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(es1ReadAcl, Action.ADD))
        val dstReadAcl = Acl(AclKey(dst.id), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(dstReadAcl, Action.ADD))
        val edgeReadAcl = Acl(AclKey(edge.id), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(edgeReadAcl, Action.ADD))

        loginAs("user1")

        val noEntitySetData7 = searchApi.searchEntitySetData(simpleSearchConstraint)
        Assert.assertEquals(0L, noEntitySetData7.numHits)
        Assert.assertEquals(0, noEntitySetData7.hits.size)
        val noEntitySetData8 = searchApi.executeEntitySetDataQuery(esLinking.id, searchTerm)
        Assert.assertEquals(0L, noEntitySetData8.numHits)
        Assert.assertEquals(0, noEntitySetData8.hits.size)
        val noEntitySetData9 = searchApi.executeAdvancedEntitySetDataQuery(esLinking.id, advancedSearchTerm)
        Assert.assertEquals(0L, noEntitySetData9.numHits)
        Assert.assertEquals(0, noEntitySetData9.hits.size)

        val noNeighborData7 = searchApi.executeEntityNeighborSearch(esLinking.id, linkingId)
        Assert.assertEquals(0, noNeighborData7.size)
        val noNeighborData8 = searchApi.executeFilteredEntityNeighborSearch(esLinking.id, neighborsFilter)
        Assert.assertEquals(0, noNeighborData8.size)
        val noNeighborData9 = searchApi.executeFilteredEntityNeighborIdsSearch(esLinking.id, neighborsFilter)
        Assert.assertEquals(0, noNeighborData9.size)

        loginAs("admin")


        // try to read data with permission on both normal entitysets but no properties
        val es2ReadAcl = Acl(AclKey(esId2), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(es2ReadAcl, Action.ADD))

        loginAs("user1")

        val noEntitySetData10 = searchApi.searchEntitySetData(simpleSearchConstraint)
        Assert.assertEquals(0L, noEntitySetData10.numHits)
        Assert.assertEquals(0, noEntitySetData10.hits.size)

        val noEntitySetData11 = searchApi.executeEntitySetDataQuery(esLinking.id, searchTerm)
        Assert.assertEquals(0L, noEntitySetData11.numHits)
        Assert.assertEquals(0, noEntitySetData11.hits.size)

        val noEntitySetData12 = searchApi.executeAdvancedEntitySetDataQuery(esLinking.id, advancedSearchTerm)
        Assert.assertEquals(0L, noEntitySetData12.numHits)
        Assert.assertEquals(0, noEntitySetData12.hits.size)

        val noNeighborData10 = searchApi.executeEntityNeighborSearch(esLinking.id, linkingId)
        Assert.assertFalse(noNeighborData10.isEmpty())
        Assert.assertEquals(setOf(EdmConstants.ID_FQN), noNeighborData10[0].associationDetails.keys)
        Assert.assertEquals(setOf(EdmConstants.ID_FQN), noNeighborData10[0].neighborDetails.get().keys)

        val noNeighborData11 = searchApi.executeFilteredEntityNeighborSearch(esLinking.id, neighborsFilter)
        Assert.assertEquals(linkingIds.size, noNeighborData11.size)
        Assert.assertTrue(noNeighborData11[linkingIds.random()]!!.isNotEmpty())
        Assert.assertEquals(
                setOf(EdmConstants.ID_FQN),
                noNeighborData11[linkingIds.random()]!![0].associationDetails.keys)
        Assert.assertEquals(
                setOf(EdmConstants.ID_FQN),
                noNeighborData11[linkingIds.random()]!![0].neighborDetails.get().keys)

        val noNeighborData12 = searchApi.executeFilteredEntityNeighborIdsSearch(esLinking.id, neighborsFilter)
        Assert.assertEquals(linkingIds.size, noNeighborData12.size)
        Assert.assertTrue(noNeighborData12[linkingIds.random()]!!.isNotEmpty())
        Assert.assertTrue(noNeighborData12[linkingIds.random()]!![edge.id]!!.size() > 0)
        Assert.assertEquals(setOf(dst.id), noNeighborData12[linkingIds.random()]!![edge.id]!!.keySet())

        loginAs("admin")


        // try to read with permission on a property type for just 1 entityset and connected entity sets
        val personPtReadAcl1 = Acl(AclKey(esId1, personPt), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(personPtReadAcl1, Action.ADD))
        val ptReadAcl2 = Acl(AclKey(dst.id, pt), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(ptReadAcl2, Action.ADD))
        val aptReadAcl = Acl(AclKey(edge.id, apt), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(aptReadAcl, Action.ADD))

        loginAs("user1")

        val noPtData1 = searchApi.searchEntitySetData(simpleSearchConstraint)
        Assert.assertEquals(0L, noPtData1.numHits)
        Assert.assertEquals(0, noPtData1.hits.size)

        val noPtData2 = searchApi.executeEntitySetDataQuery(esLinking.id, searchTerm)
        Assert.assertEquals(0L, noPtData2.numHits)
        Assert.assertEquals(0, noPtData2.hits.size)

        val noPtData3 = searchApi.executeAdvancedEntitySetDataQuery(esLinking.id, advancedSearchTerm)
        Assert.assertEquals(0L, noPtData3.numHits)
        Assert.assertEquals(0, noPtData3.hits.size)

        val noNeighborPtData1 = searchApi.executeEntityNeighborSearch(esLinking.id, linkingId)
        Assert.assertTrue(noNeighborPtData1.isNotEmpty())
        Assert.assertEquals(
                setOf(EdmConstants.ID_FQN, associationPropertyType.type),
                noNeighborPtData1[0].associationDetails.keys)
        Assert.assertEquals(
                setOf(EdmConstants.ID_FQN, propertyType.type),
                noNeighborPtData1[0].neighborDetails.get().keys)

        val noNeighborPtData2 = searchApi.executeFilteredEntityNeighborSearch(esLinking.id, neighborsFilter)
        Assert.assertEquals(linkingIds.size, noNeighborPtData2.size)
        Assert.assertEquals(
                setOf(EdmConstants.ID_FQN, associationPropertyType.type),
                noNeighborPtData2[linkingIds.random()]!![0].associationDetails.keys)
        Assert.assertEquals(
                setOf(EdmConstants.ID_FQN, propertyType.type),
                noNeighborPtData2[linkingIds.random()]!![0].neighborDetails.get().keys)

        val noNeighborPtData3 = searchApi.executeFilteredEntityNeighborIdsSearch(esLinking.id, neighborsFilter)
        Assert.assertEquals(linkingIds.size, noNeighborPtData3.size)
        Assert.assertTrue(noNeighborPtData3[linkingIds.random()]!!.isNotEmpty())
        Assert.assertTrue(noNeighborPtData3[linkingIds.random()]!![edge.id]!!.size() > 0)
        Assert.assertEquals(setOf(dst.id), noNeighborPtData3[linkingIds.random()]!![edge.id]!!.keySet())

        loginAs("admin")


        // try to read with permission on a property type for both normal entitysets
        val personPtReadAcl3 = Acl(AclKey(esId2, personPt), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(personPtReadAcl3, Action.ADD))


        loginAs("user1")

        val ptData1 = searchApi.searchEntitySetData(simpleSearchConstraint)
        Assert.assertTrue(ptData1.numHits > 0)
        Assert.assertEquals(linkingIds.size, ptData1.hits.size)
        Assert.assertEquals(setOf(EdmConstants.ID_FQN, personPropertyType.type), ptData1.hits[0].keys)

        val ptData2 = searchApi.executeEntitySetDataQuery(esLinking.id, searchTerm)
        Assert.assertTrue(ptData2.numHits > 0)
        Assert.assertEquals(linkingIds.size, ptData2.hits.size)
        Assert.assertEquals(setOf(EdmConstants.ID_FQN, personPropertyType.type), ptData2.hits[0].keys)

        val ptData3 = searchApi.executeAdvancedEntitySetDataQuery(esLinking.id, advancedSearchTerm)
        Assert.assertTrue(ptData3.numHits > 0)
        Assert.assertEquals(linkingIds.size, ptData3.hits.size)
        Assert.assertEquals(setOf(EdmConstants.ID_FQN, personPropertyType.type), ptData3.hits[0].keys)

        val neighborData1 = searchApi.executeEntityNeighborSearch(esLinking.id, linkingId)
        Assert.assertTrue(neighborData1.isNotEmpty())
        Assert.assertEquals(
                setOf(EdmConstants.ID_FQN, associationPropertyType.type),
                neighborData1[0].associationDetails.keys)
        Assert.assertEquals(
                setOf(EdmConstants.ID_FQN, propertyType.type),
                neighborData1[0].neighborDetails.get().keys)

        val neighborData2 = searchApi.executeFilteredEntityNeighborSearch(esLinking.id, neighborsFilter)
        Assert.assertEquals(linkingIds.size, neighborData2.size)
        Assert.assertEquals(
                setOf(EdmConstants.ID_FQN, associationPropertyType.type),
                neighborData2[linkingIds.random()]!![0].associationDetails.keys)
        Assert.assertEquals(
                setOf(EdmConstants.ID_FQN, propertyType.type),
                neighborData2[linkingIds.random()]!![0].neighborDetails.get().keys)

        val neighborData3 = searchApi.executeFilteredEntityNeighborIdsSearch(esLinking.id, neighborsFilter)
        Assert.assertEquals(linkingIds.size, neighborData3.size)
        Assert.assertTrue(neighborData3[linkingIds.random()]!!.isNotEmpty())
        Assert.assertTrue(neighborData3[linkingIds.random()]!![edge.id]!!.size() > 0)
        Assert.assertEquals(setOf(dst.id), neighborData3[linkingIds.random()]!![edge.id]!!.keySet())

        loginAs("admin")


        // give read permission on all property types
        personEt.properties.forEach {
            permissionsApi.updateAcl(AclData(
                    Acl(AclKey(esId1, it), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))),
                    Action.ADD))
            permissionsApi.updateAcl(AclData(
                    Acl(AclKey(esId2, it), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))),
                    Action.ADD))
        }
        et.properties.forEach {
            permissionsApi.updateAcl(AclData(
                    Acl(AclKey(dst.id, it), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))),
                    Action.ADD))
        }

        at.properties.forEach {
            permissionsApi.updateAcl(AclData(
                    Acl(AclKey(edge.id, it), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))),
                    Action.ADD))
        }

        loginAs("user1")

        val searchResult1 = searchApi.searchEntitySetData(simpleSearchConstraint)
        Assert.assertTrue(searchResult1.numHits > 0)
        Assert.assertEquals(linkingIds.size, searchResult1.hits.size)
        Assert.assertEquals(
                personPropertiesWithData.map { edmApi.getPropertyType(it).type }.toSet()
                        + setOf(EdmConstants.ID_FQN),
                searchResult1.hits[0].keys)

        val searchResult2 = searchApi.executeEntitySetDataQuery(esLinking.id, searchTerm)
        Assert.assertTrue(searchResult2.numHits > 0)
        Assert.assertEquals(linkingIds.size, searchResult2.hits.size)
        Assert.assertEquals(
                personPropertiesWithData.map { edmApi.getPropertyType(it).type }.toSet()
                        + setOf(EdmConstants.ID_FQN),
                searchResult2.hits[0].keys)

        val searchResult3 = searchApi.executeAdvancedEntitySetDataQuery(esLinking.id, advancedSearchTerm)
        Assert.assertTrue(searchResult3.numHits > 0)
        Assert.assertEquals(linkingIds.size, searchResult3.hits.size)
        Assert.assertEquals(
                personPropertiesWithData.map { edmApi.getPropertyType(it).type }.toSet()
                        + setOf(EdmConstants.ID_FQN),
                searchResult3.hits[0].keys)

        val neighborData4 = searchApi.executeEntityNeighborSearch(esLinking.id, linkingId)
        Assert.assertTrue(neighborData4.size > 0)
        Assert.assertEquals(
                at.properties.map { edmApi.getPropertyType(it).type }.toSet() + setOf(EdmConstants.ID_FQN),
                neighborData4[0].associationDetails.keys)
        Assert.assertEquals(
                et.properties.map { edmApi.getPropertyType(it).type }.toSet() + setOf(EdmConstants.ID_FQN),
                neighborData4[0].neighborDetails.get().keys)

        val neighborData5 = searchApi.executeFilteredEntityNeighborSearch(esLinking.id, neighborsFilter)
        Assert.assertEquals(linkingIds.size, neighborData5.size)
        Assert.assertEquals(
                at.properties.map { edmApi.getPropertyType(it).type }.toSet() + setOf(EdmConstants.ID_FQN),
                neighborData5[linkingIds.random()]!![0].associationDetails.keys)
        Assert.assertEquals(
                et.properties.map { edmApi.getPropertyType(it).type }.toSet() + setOf(EdmConstants.ID_FQN),
                neighborData5[linkingIds.random()]!![0].neighborDetails.get().keys)

        val neighborData6 = searchApi.executeFilteredEntityNeighborIdsSearch(esLinking.id, neighborsFilter)
        Assert.assertEquals(linkingIds.size, neighborData6.size)
        Assert.assertTrue(neighborData6[linkingIds.random()]!!.isNotEmpty())
        Assert.assertTrue(neighborData6[linkingIds.random()]!![edge.id]!!.size() > 0)
        Assert.assertEquals(setOf(dst.id), neighborData6[linkingIds.random()]!![edge.id]!!.keySet())

        loginAs("admin")

        entitySetsApi.deleteEntitySet(esLinking.id)
    }

}