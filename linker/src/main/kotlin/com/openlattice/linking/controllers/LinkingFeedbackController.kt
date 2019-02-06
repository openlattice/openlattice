package com.openlattice.linking.controllers

import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.data.EntityDataKey
import com.openlattice.data.EntityDatastore
import com.openlattice.data.storage.PostgresDataManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.linking.*
import com.openlattice.linking.util.PersonMetric
import com.openlattice.linking.util.PersonProperties
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import java.lang.IllegalArgumentException
import javax.inject.Inject

@RestController
@RequestMapping(LinkingFeedbackApi.CONTROLLER)
class LinkingFeedbackController
@Inject
constructor(
        hazelcastInstance: HazelcastInstance,
        private val authorizationManager: AuthorizationManager,
        private val feedbackQueryService: PostgresLinkingFeedbackQueryService,
        private val matcher: Matcher,
        private val dataLoader: DataLoader,
        private val edm: EdmManager,
        private val entityDataStore: EntityDatastore,
        private val dataManager: PostgresDataManager
) : LinkingFeedbackApi, AuthorizingComponent {

    private val linkingFeedbacks = hazelcastInstance.getMap<EntityKeyPair, Boolean>(HazelcastMap.LINKING_FEEDBACKS.name)
    private val personPropertyIds = PersonProperties.FQNS.map { edm.getPropertyTypeId(it) }

    companion object {
        private val logger = LoggerFactory.getLogger(LinkingFeedbackController::class.java)
    }

    @PutMapping(path = [LinkingFeedbackApi.FEEDBACK])
    override fun addLinkingFeedback(@RequestBody feedback: LinkingFeedback): Int {
        // ensure read access on linking entity set
        ensureReadAccess(AclKey(feedback.linkingEntityDataKey.entitySetId))

        // ensure read access on all entity sets involved and the properties used for linking
        // make sure, that all entitysets are part of the linking entity set and entity ids have the provided linking id
        linkingFeedbackCheck(
                feedback.linkingEntities + feedback.nonLinkingEntities,
                feedback.linkingEntityDataKey)


        // add feedbacks
        val linkingEntitiesList = feedback.linkingEntities.toList()
        val nonLinkingEntitiesList = feedback.nonLinkingEntities.toList()

        var positiveFeedbackCount = 0
        var negativeFeedbackCount = 0


        feedback.linkingEntities.forEachIndexed { index, linkingEntity ->
            // generate pairs between linking entities themselves
            positiveFeedbackCount += createLinkingFeedbackCombinations(
                    linkingEntity, linkingEntitiesList, index + 1, true)

            // generate pairs between linking and non-linking entities
            negativeFeedbackCount += createLinkingFeedbackCombinations(
                    linkingEntity, nonLinkingEntitiesList, 0, false)
        }

        // mark entities as need to be linked
        dataManager.markAsNeedsToBeLinked(feedback.linkingEntities + feedback.nonLinkingEntities)


        logger.info("Submitted $positiveFeedbackCount positive and $negativeFeedbackCount negative feedbacks for " +
                "linking id ${feedback.linkingEntityDataKey.entityKeyId}")

        return positiveFeedbackCount + negativeFeedbackCount
    }

    private fun linkingFeedbackCheck(entityDataKeys: Set<EntityDataKey>, linkingEntityDataKey: EntityDataKey) {
        val linkingEntitySet = edm.getEntitySet(linkingEntityDataKey.entitySetId)
        val entityKeyIdsOfLinkingId = entityDataStore
                .getEntityKeyIdsOfLinkingIds(setOf(linkingEntityDataKey.entityKeyId)).first().right

        entityDataKeys.forEach { entityDataKey ->
            if (!linkingEntitySet.linkedEntitySets.contains(entityDataKey.entitySetId)) {
                throw IllegalArgumentException(
                        "Feedback can only be submitted for entities contained by linking entity set")
            }
            if (!entityKeyIdsOfLinkingId.contains(entityDataKey.entityKeyId)) {
                throw IllegalArgumentException(
                        "Feedback can only be submitted for entities with same linking id")
            }

            ensureReadAccess(AclKey(entityDataKey.entitySetId))
            personPropertyIds.forEach { ensureReadAccess(AclKey(entityDataKey.entitySetId, it)) }
        }
    }

    private fun createLinkingFeedbackCombinations(
            entityDataKey: EntityDataKey, entityList: List<EntityDataKey>, offset: Int, linked: Boolean): Int {
        return (offset until entityList.size).map {
            addLinkingFeedback(EntityLinkingFeedback(EntityKeyPair(entityDataKey, entityList[it]), linked))
        }.sum()
    }

    private fun addLinkingFeedback(entityLinkingFeedback: EntityLinkingFeedback): Int {
        linkingFeedbacks.set(entityLinkingFeedback.entityPair, entityLinkingFeedback.linked)
        val count = feedbackQueryService.addLinkingFeedback(entityLinkingFeedback)

        logger.info("Linking feedback submitted for entities: ${entityLinkingFeedback.entityPair.first} - " +
                "${entityLinkingFeedback.entityPair.second}, linked = ${entityLinkingFeedback.linked}")

        return count
    }

    @GetMapping(path = [LinkingFeedbackApi.FEEDBACK + LinkingFeedbackApi.ALL])
    override fun getAllLinkingFeedbacks(): Iterable<EntityLinkingFeedback> {
        return feedbackQueryService.getLinkingFeedbacks()
    }

    @GetMapping(path = [LinkingFeedbackApi.FEEDBACK + LinkingFeedbackApi.ALL + LinkingFeedbackApi.FEATURES])
    override fun getAllLinkingFeedbacksWithFeatures(): Iterable<EntityLinkingFeatures> {
        return feedbackQueryService.getLinkingFeedbacks().map {
            val entities = dataLoader.getEntities(setOf(it.entityPair.first, it.entityPair.second))
            EntityLinkingFeatures(
                    it,
                    PersonMetric.values().map { it.toString() }
                            .zip(matcher.extractFeatures(
                                    matcher.extractProperties(entities.getValue(it.entityPair.first)),
                                    matcher.extractProperties(entities.getValue(it.entityPair.second))).asList())
                            .toMap()
            )
        }
    }

    @PostMapping(path = [LinkingFeedbackApi.FEEDBACK + LinkingFeedbackApi.FEATURES])
    override fun getLinkingFeedbackWithFeatures(
            @RequestBody entityPair: EntityKeyPair): EntityLinkingFeatures? {
        val feedback = checkNotNull(feedbackQueryService.getLinkingFeedback(entityPair))
        { "Linking feedback for entities ${entityPair.first} - ${entityPair.second} does not exist" }

        val entities = dataLoader.getEntities(setOf(feedback.entityPair.first, feedback.entityPair.second))
        return EntityLinkingFeatures(
                feedback,
                PersonMetric.values().map { it.toString() }
                        .zip(matcher.extractFeatures(
                                matcher.extractProperties(entities.getValue(feedback.entityPair.first)),
                                matcher.extractProperties(entities.getValue(feedback.entityPair.second))).asList())
                        .toMap()
        )
    }


    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}