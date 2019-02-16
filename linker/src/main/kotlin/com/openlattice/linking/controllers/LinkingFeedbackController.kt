package com.openlattice.linking.controllers

import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.data.EntityDataKey
import com.openlattice.data.EntityDatastore
import com.openlattice.data.storage.PostgresDataManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.linking.*
import com.openlattice.linking.util.PersonMetric
import com.openlattice.linking.util.PersonProperties
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import retrofit2.http.Body
import java.lang.IllegalArgumentException
import javax.inject.Inject

@RestController
@RequestMapping(LinkingFeedbackApi.CONTROLLER)
class LinkingFeedbackController
@Inject
constructor(
        private val authorizationManager: AuthorizationManager,
        private val feedbackService: PostgresLinkingFeedbackService,
        private val matcher: Matcher,
        private val dataLoader: DataLoader,
        private val edm: EdmManager,
        private val entityDataStore: EntityDatastore,
        private val dataManager: PostgresDataManager
) : LinkingFeedbackApi, AuthorizingComponent {


    private val personPropertyIds = PersonProperties.FQNS.map { edm.getPropertyTypeId(it) }

    companion object {
        private val logger = LoggerFactory.getLogger(LinkingFeedbackController::class.java)
    }

    @PutMapping(path = [LinkingFeedbackApi.FEEDBACK])
    override fun addLinkingFeedback(@RequestBody feedback: LinkingFeedback): Int {
        if (feedback.linkingEntities.isEmpty() || feedback.linkingEntities.size + feedback.nonLinkingEntities.size < 2) {
            throw IllegalArgumentException(
                    "Cannot submit feedback for less than 2 entities or if no positively linking entity is provided")
        }

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


        linkingEntitiesList.forEachIndexed { index, linkingEntity ->
            // generate pairs between linking entities themselves
            positiveFeedbackCount += createLinkingFeedbackCombinations(
                    linkingEntity, linkingEntitiesList, index, true)

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
        (offset until entityList.size).forEach {
            addLinkingFeedback(EntityLinkingFeedback(EntityKeyPair(entityDataKey, entityList[it]), linked))
        }
        return entityList.size
    }

    private fun addLinkingFeedback(entityLinkingFeedback: EntityLinkingFeedback) {

        feedbackService.addLinkingFeedback(entityLinkingFeedback)

        logger.info("Linking feedback submitted for entities: ${entityLinkingFeedback.entityPair.first} - " +
                "${entityLinkingFeedback.entityPair.second}, linked = ${entityLinkingFeedback.linked}")
    }

    @GetMapping(path = [LinkingFeedbackApi.FEEDBACK + LinkingFeedbackApi.ENTITY])
    override fun getLinkingFeedbacksOnEntity(
            @RequestParam(value = LinkingFeedbackApi.FEEDBACK_TYPE, required = false) feedbackType: FeedbackType,
            @RequestBody entity: EntityDataKey): Iterable<EntityLinkingFeedback> {
        return feedbackService.getLinkingFeedbackOnEntity(feedbackType, entity)
    }

    @GetMapping(path = [LinkingFeedbackApi.FEEDBACK + LinkingFeedbackApi.ALL])
    override fun getAllLinkingFeedbacks(): Iterable<EntityLinkingFeedback> {
        return feedbackService.getLinkingFeedbacks()
    }

    @GetMapping(path = [LinkingFeedbackApi.FEEDBACK + LinkingFeedbackApi.ALL + LinkingFeedbackApi.FEATURES])
    override fun getAllLinkingFeedbacksWithFeatures(): Iterable<EntityLinkingFeatures> {
        return feedbackService.getLinkingFeedbacks().map {
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
        val feedback = checkNotNull(feedbackService.getLinkingFeedback(entityPair))
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

    @DeleteMapping( path = [LinkingFeedbackApi.FEEDBACK] )
    override fun deleteLinkingFeedback(@RequestBody entityPair: EntityKeyPair) {
        feedbackService.deleteLinkingFeedback(entityPair)
    }


    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}