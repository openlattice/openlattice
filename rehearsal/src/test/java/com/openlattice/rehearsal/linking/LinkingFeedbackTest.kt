/*
 * Copyright (C) 2019. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.rehearsal.linking

import com.google.common.collect.ImmutableSet
import com.openlattice.data.EntityDataKey
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.linking.FeedbackType
import com.openlattice.linking.LinkingFeedback
import com.openlattice.rehearsal.SetupTestData
import com.openlattice.rehearsal.edm.PERSON_NAME
import com.openlattice.rehearsal.edm.PERSON_NAMESPACE
import com.openlattice.search.requests.SearchConstraints
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.*
import org.junit.runners.MethodSorters
import org.slf4j.LoggerFactory
import java.lang.reflect.UndeclaredThrowableException
import java.util.*
import kotlin.random.Random

/**
 * Test for [com.openlattice.linking.LinkingFeedbackApi]
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class LinkingFeedbackTest : SetupTestData() {
    companion object {
        private val logger = LoggerFactory.getLogger(LinkingFeedbackTest::class.java)

        private val importedEntitySets = mapOf(
                "SocratesTestA" to Pair("socratesA.yaml", "test_linked_person_1.csv"),
                "SocratesTestB" to Pair("socratesB.yaml", "test_linked_person_2.csv"),
                "SocratesTestC" to Pair("socratesC.yaml", "test_linked_person_3.csv"),
                "SocratesTestD" to Pair("socratesD.yaml", "test_linked_person_4.csv"))

        lateinit var personEt: EntityType
        lateinit var linkingEntitySet: EntitySet

        @JvmStatic
        @BeforeClass
        fun init() {
            importedEntitySets.forEach {
                importDataSet(it.value.first, it.value.second)
            }

            loginAs("admin")
            personEt = edmApi.getEntityType(edmApi.getEntityTypeId(PERSON_NAMESPACE, PERSON_NAME))

            val importedEntitySetKeysIterator = importedEntitySets.keys.iterator()
            linkingEntitySet = createEntitySet(
                    personEt,
                    true,
                    setOf(edmApi.getEntitySetId(importedEntitySetKeysIterator.next()),
                            edmApi.getEntitySetId(importedEntitySetKeysIterator.next()),
                            edmApi.getEntitySetId(importedEntitySetKeysIterator.next()),
                            edmApi.getEntitySetId(importedEntitySetKeysIterator.next()))
            )

            waitForBackgroundServices()
        }

        @JvmStatic
        @AfterClass
        fun tearDown() {
            // delete all created entity sets
            (importedEntitySets.keys + linkingEntitySet.name).forEach {
                try {
                    edmApi.deleteEntitySet(edmApi.getEntitySetId(it))
                } catch (e: UndeclaredThrowableException) {
                }
            }

            // delete all feedbacks
            linkingFeedbackApi.getAllLinkingFeedbacks().forEach {
                linkingFeedbackApi.deleteLinkingFeedback(it.entityPair)
            }
        }

        private fun waitForBackgroundServices() {
            // wait for re-linking to finish
            Thread.sleep(20000L)
            while (!checkLinkingFinished(importedEntitySets.keys)) {
                Thread.sleep(5000L)
            }
            Thread.sleep(60000L) // wait for indexing to finish
        }
    }

    /**
     * For these tests, we assume, that the 4 linked entity sets, which contain 1 person only are all matched together
     */

    @Test
    fun testA_addPositiveFeedback() {
        val linkedData = searchApi.searchEntitySetData(
                SearchConstraints.simpleSearchConstraints(
                        arrayOf(linkingEntitySet.id), 0, 100, "*")).hits
        val linkingId = linkedData.map {
            UUID.fromString(it[FullQualifiedName("openlattice.@id")].first() as String)
        }.toSet().first()
        val matchedEntities = realtimeLinkingApi.getMatchedEntitiesForLinkingId(linkingId)

        // skip 0 or 1 randomly, so not all of them gets positive feedback all the time
        val allEntities = ImmutableSet.copyOf(matchedEntities.flatMap { setOf(it.entityPair.first, it.entityPair.second) })
        val linkedEntities = allEntities.drop(Random.nextInt(2)).toSet()
        linkingFeedbackApi.addLinkingFeedback(
                LinkingFeedback(EntityDataKey(linkingEntitySet.id, linkingId), linkedEntities, setOf()))

        waitForBackgroundServices()

        val oldMatchedEntities = matchedEntities.map { it.entityPair to it.match }.toMap()
        val newlyMatchedEntities = realtimeLinkingApi.getMatchedEntitiesForLinkingId(linkingId)
        newlyMatchedEntities.forEach {
            val oldMatches = oldMatchedEntities[it.entityPair]
            Assert.assertTrue(oldMatches!! <= it.match)
            if (linkedEntities.contains(it.entityPair.first) && linkedEntities.contains(it.entityPair.second)) {
                Assert.assertEquals(it.match, 1.0, 1e-5)
            }
        }
    }

    @Test
    fun testB_addNegativeFeedback() {
        val linkedData = searchApi.searchEntitySetData(
                SearchConstraints.simpleSearchConstraints(
                        arrayOf(linkingEntitySet.id), 0, 100, "*")).hits
        val linkingId = linkedData.map {
            UUID.fromString(it[FullQualifiedName("openlattice.@id")].first() as String)
        }.toSet().first()
        val matchedEntities = realtimeLinkingApi.getMatchedEntitiesForLinkingId(linkingId)

        val allEntities = ImmutableSet.copyOf(matchedEntities.flatMap { setOf(it.entityPair.first, it.entityPair.second) }).toList()
        val nonLinkingEntity = allEntities[Random.nextInt(allEntities.size)]
        val linkedEntities = (allEntities - nonLinkingEntity).drop(Random.nextInt(2)).toSet()

        linkingFeedbackApi.addLinkingFeedback(
                LinkingFeedback(
                        EntityDataKey(linkingEntitySet.id, linkingId),
                        linkedEntities,
                        setOf(nonLinkingEntity)))

        waitForBackgroundServices()

        val newlyMatchedEntities = realtimeLinkingApi.getMatchedEntitiesForLinkingId(linkingId)
        val oldMatchedEntities = matchedEntities.map { it.entityPair to it.match }.toMap()

        Assert.assertFalse(newlyMatchedEntities.flatMap { setOf(it.entityPair.first, it.entityPair.second) }
                .contains(nonLinkingEntity))
        newlyMatchedEntities.forEach {
            val oldMatches = oldMatchedEntities[it.entityPair]
            Assert.assertTrue(oldMatches!! <= it.match)
            if (linkedEntities.contains(it.entityPair.first) && linkedEntities.contains(it.entityPair.second)) {
                Assert.assertEquals(it.match, 1.0, 1e-5)
            }
        }
    }

    @Test
    fun testC_getAllFeedbacks() {
        val linkedData = searchApi.searchEntitySetData(
                SearchConstraints.simpleSearchConstraints(
                        arrayOf(linkingEntitySet.id), 0, 100, "*")).hits
        val linkingId = linkedData.map {
            UUID.fromString(it[FullQualifiedName("openlattice.@id")].first() as String)
        }.toSet().first()
        val matchedEntities = realtimeLinkingApi.getMatchedEntitiesForLinkingId(linkingId)

        // link all positively
        val linkedEntities = ImmutableSet.copyOf(matchedEntities.flatMap { setOf(it.entityPair.first, it.entityPair.second) })
        linkingFeedbackApi.addLinkingFeedback(
                LinkingFeedback(
                        EntityDataKey(linkingEntitySet.id, linkingId),
                        linkedEntities,
                        setOf()))

        waitForBackgroundServices()

        val feedbacks = linkingFeedbackApi.getAllLinkingFeedbacks().toSet()
        val feedbacksWithFeatures = linkingFeedbackApi.getAllLinkingFeedbacksWithFeatures().toSet()

        Assert.assertEquals(feedbacks.size, feedbacksWithFeatures.size)
        Assert.assertEquals(10, feedbacks.size) // we have 4 entities: (4 * 5) / 2
        Assert.assertEquals(feedbacks, feedbacksWithFeatures.map { it.entityLinkingFeedback }.toSet())

        val linkedDataProperties = searchApi.searchEntitySetData(
                SearchConstraints.simpleSearchConstraints(
                        arrayOf(linkingEntitySet.id), 0, 100, "*")).hits
                .flatMap { it.asMap().map { it.key.fullQualifiedNameAsString } }
        logger.info("$linkedDataProperties")
        val feedbackFeatures = feedbacksWithFeatures.map { it.features.filter { it.value > 0.0 } }
                .flatMap { it.keys }.toSet()
        logger.info("$feedbackFeatures")
    }

    @Test
    fun testD_getFeedbacksForEntity() {
        // Redo linking feedback
        val linkedData = searchApi.searchEntitySetData(
                SearchConstraints.simpleSearchConstraints(
                        arrayOf(linkingEntitySet.id), 0, 100, "*")).hits
        val linkingId = linkedData.map {
            UUID.fromString(it[FullQualifiedName("openlattice.@id")].first() as String)
        }.toSet().first()
        val matchedEntities = realtimeLinkingApi.getMatchedEntitiesForLinkingId(linkingId)

        val allEntities = ImmutableSet.copyOf(matchedEntities.flatMap { setOf(it.entityPair.first, it.entityPair.second) }).toList()
        val nonLinkingEntity = allEntities[Random.nextInt(allEntities.size)]
        val linkedEntities = (allEntities - nonLinkingEntity).drop(Random.nextInt(2)).toSet()
        linkingFeedbackApi.addLinkingFeedback(
                LinkingFeedback(
                        EntityDataKey(linkingEntitySet.id, linkingId),
                        linkedEntities,
                        setOf(nonLinkingEntity)))

        waitForBackgroundServices()

        // feedback on positive
        val matchedEntityPairs = matchedEntities.map { it.entityPair }.toSet()
        val linkingEntity = linkedEntities.toList()[Random.nextInt(linkedEntities.size)]
        val positiveFeedbacks = linkingFeedbackApi.getLinkingFeedbacksOnEntity(FeedbackType.Positive, linkingEntity)
        positiveFeedbacks.forEach {
            Assert.assertTrue(matchedEntityPairs.contains(it.entityPair))
            Assert.assertTrue(it.linked)
        }

        // feedback on negative
        val negativeFeedbacks = linkingFeedbackApi.getLinkingFeedbacksOnEntity(FeedbackType.Negative, nonLinkingEntity)
        val negativeFeedbackPair = negativeFeedbacks.map {
            val otherEntity = if (it.entityPair.first == nonLinkingEntity) it.entityPair.second else it.entityPair.first
            Assert.assertFalse(it.linked)
            return@map otherEntity
        }.toSet()

        // there is a negative feedback pair for all linked entities
        Assert.assertEquals(linkedEntities, negativeFeedbackPair)

        // feedback on random
        val randomFeedbacks = linkingFeedbackApi.getLinkingFeedbacksOnEntity(
                FeedbackType.All,
                (linkedEntities + nonLinkingEntity).toList()[Random.nextInt(linkedEntities.size + 1)])
        Assert.assertTrue(randomFeedbacks.map { it.linked }.contains(true))
        Assert.assertTrue(randomFeedbacks.map { it.linked }.contains(false))
    }
}