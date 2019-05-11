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
import com.openlattice.postgres.DataTables
import com.openlattice.rehearsal.SetupTestData
import com.openlattice.rehearsal.edm.EdmTestConstants
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
 *
 * Note: tests need to run in order
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
        lateinit var linkingId: UUID
        lateinit var allEntities: List<EntityDataKey>
        lateinit var nonLinkingEntity: EntityDataKey

        @JvmStatic
        @BeforeClass
        fun init() {
            importedEntitySets.forEach {
                importDataSet(it.value.first, it.value.second)
            }

            loginAs("admin")
            personEt = EdmTestConstants.personEt

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
        linkingId = linkedData.map {
            UUID.fromString(it[DataTables.ID_FQN].first() as String)
        }.toSet().first()
        val matchedEntities = realtimeLinkingApi.getMatchedEntitiesForLinkingId(linkingId)

        // skip 1, so not all of them gets positive feedback
        allEntities = ImmutableSet.copyOf(matchedEntities
                .flatMap { setOf(it.entityPair.first, it.entityPair.second) }).toList()
        val linkedEntities = allEntities.drop(1).toSet()
        linkingFeedbackApi.addLinkingFeedback(
                LinkingFeedback(EntityDataKey(linkingEntitySet.id, linkingId), linkedEntities, setOf()))
        waitForBackgroundServices()

        val oldMatchedEntities = matchedEntities.map { it.entityPair to it.match }.toMap()
        val newlyMatchedEntities = realtimeLinkingApi.getMatchedEntitiesForLinkingId(linkingId)
                .map { it.entityPair to it.match }.toMap()
        newlyMatchedEntities.forEach {
            val oldMatches = oldMatchedEntities[it.key]
            Assert.assertTrue(oldMatches!! <= it.value)
            if (linkedEntities.contains(it.key.first) && linkedEntities.contains(it.key.second)
                    && it.key.second != it.key.first) {
                Assert.assertEquals(it.value, 1.0, 1e-5)
            }
        }

        // add pos feedback for all
        linkingFeedbackApi.addLinkingFeedback(
                LinkingFeedback(EntityDataKey(linkingEntitySet.id, linkingId), allEntities.toSet(), setOf()))
        waitForBackgroundServices()

        val newNewlyMatchedEntities = realtimeLinkingApi.getMatchedEntitiesForLinkingId(linkingId)
        newNewlyMatchedEntities.forEach {
            val oldMatches = newlyMatchedEntities[it.entityPair]
            Assert.assertTrue(oldMatches!! <= it.match)
            if (linkedEntities.contains(it.entityPair.first) && linkedEntities.contains(it.entityPair.second)
                    && it.entityPair.second != it.entityPair.first) {
                Assert.assertEquals(it.match, 1.0, 1e-5)
            }
        }
    }

    @Test
    fun testB_addNegativeFeedback() {
        val matchedEntities = realtimeLinkingApi.getMatchedEntitiesForLinkingId(linkingId)

        nonLinkingEntity = allEntities[Random.nextInt(allEntities.size)]
        val linkedEntities = (allEntities - nonLinkingEntity).toSet()

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
            if (linkedEntities.contains(it.entityPair.first) && linkedEntities.contains(it.entityPair.second)
                    && it.entityPair.second != it.entityPair.first) {
                Assert.assertEquals(it.match, 1.0, 1e-5)
            }
        }
    }

    @Test
    fun testC_getAllFeedbacks() {
        val feedbacks = linkingFeedbackApi.getAllLinkingFeedbacks().toSet()
        val feedbacksWithFeatures = linkingFeedbackApi.getAllLinkingFeedbacksWithFeatures().toSet()

        logger.info("All feedbacks {}", feedbacks)
        logger.info("All feedback features{}", feedbacksWithFeatures)

        Assert.assertEquals(feedbacks.size, feedbacksWithFeatures.size)
        Assert.assertEquals(6, feedbacks.size) // we have 3 pos and 1 neg: (2*3) / 2 + 3
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
        val matchedEntities = realtimeLinkingApi.getMatchedEntitiesForLinkingId(linkingId)
        val linkedEntities = (allEntities - nonLinkingEntity).toSet()

        // feedback on positive
        val matchedEntityPairs = matchedEntities.map { it.entityPair }.toSet()
        val linkingEntity = linkedEntities.toList()[Random.nextInt(linkedEntities.size)]
        val positiveFeedbacks = linkingFeedbackApi
                .getLinkingFeedbacksOnEntity(FeedbackType.Positive, linkingEntity).toList()
        // 3 positive feedbacks, any of the entities is in 2 of them
        Assert.assertEquals(2, positiveFeedbacks.size)
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

    @Test
    fun testE_IllegalArguments() {
        // test not enough entities provided
        try{
            linkingFeedbackApi.addLinkingFeedback(LinkingFeedback(
                    EntityDataKey(linkingEntitySet.id, linkingId),
                    setOf(),
                    setOf()))
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Cannot submit feedback for less than 2 entities or if no positively linking entity is provided", true))
        }

        try{
            linkingFeedbackApi.addLinkingFeedback(LinkingFeedback(
                    EntityDataKey(linkingEntitySet.id, linkingId),
                    setOf(allEntities.first()),
                    setOf()))
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Cannot submit feedback for less than 2 entities or if no positively linking entity is provided", true))
        }

        try{
            linkingFeedbackApi.addLinkingFeedback(LinkingFeedback(
                    EntityDataKey(linkingEntitySet.id, linkingId),
                    setOf(),
                    setOf(allEntities.first(), allEntities.last())))
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Cannot submit feedback for less than 2 entities or if no positively linking entity is provided", true))
        }

        // test adding same entity to linking and nonlinking sets
        try{
            linkingFeedbackApi.addLinkingFeedback(LinkingFeedback(
                    EntityDataKey(linkingEntitySet.id, linkingId),
                    setOf(allEntities.first()),
                    setOf(allEntities.first())))
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Cannot submit feedback with and entity being both linking and non-linking", true))
        }

        // test adding feedback for entity, which is not part on linking entity set
        try{
            linkingFeedbackApi.addLinkingFeedback(LinkingFeedback(
                    EntityDataKey(linkingEntitySet.id, linkingId),
                    setOf(EntityDataKey(UUID.randomUUID(), UUID.randomUUID())),
                    setOf(EntityDataKey(UUID.randomUUID(), UUID.randomUUID()))))
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Feedback can only be submitted for entities contained by linking entity set", true))
        }

        // test adding feedback for entity, which has different linking id
        try{
            linkingFeedbackApi.addLinkingFeedback(LinkingFeedback(
                    EntityDataKey(linkingEntitySet.id, linkingId),
                    allEntities.toSet(),
                    setOf()))
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Feedback can only be submitted for entities with same linking id", true))
        }
    }
}