package com.openlattice.linking.controllers

import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.data.EntityDataKey
import com.openlattice.linking.LinkingFeedbackApi
import com.openlattice.linking.PostgresLinkingFeedbackQueryService
import com.openlattice.linking.Matcher
import com.openlattice.linking.DataLoader
import com.openlattice.linking.EntityLinkingFeedback
import com.openlattice.linking.EntityLinkingFeatures
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import javax.inject.Inject

@RestController
@RequestMapping(LinkingFeedbackApi.CONTROLLER)
class LinkingFeedbackController
@Inject
constructor(
        private val authorizationManager: AuthorizationManager,
        private val feedbackQueryService: PostgresLinkingFeedbackQueryService,
        private val matcher: Matcher,
        private val dataLoader: DataLoader
) : LinkingFeedbackApi, AuthorizingComponent {

    companion object {
        private val logger = LoggerFactory.getLogger(LinkingFeedbackController::class.java)
    }

    @PutMapping(path = [LinkingFeedbackApi.FEEDBACK])
    override fun addLinkingFeedback(@RequestBody feedback: EntityLinkingFeedback): Boolean {
        ensureReadAccess(AclKey(feedback.src.entitySetId))
        ensureReadAccess(AclKey(feedback.dst.entitySetId))

        if (feedback.src.entityKeyId == feedback.dst.entityKeyId) {
            logger.error("You cannot add feedback on linking on the same entities")
            return false
        }

        feedbackQueryService.addLinkingFeedback(feedback)
        logger.info("Linking feedback submitted for entities: ${feedback.src} - ${feedback.dst}, linked = " +
                "${feedback.linked}")
        return true
    }

    @GetMapping(path = [LinkingFeedbackApi.FEEDBACK + LinkingFeedbackApi.ALL])
    override fun getAllLinkingFeedbacks(): Iterable<EntityLinkingFeedback> {
        return feedbackQueryService.getLinkingFeedbacks()
    }

    @GetMapping(path = [LinkingFeedbackApi.FEEDBACK + LinkingFeedbackApi.ALL + LinkingFeedbackApi.FEATURES])
    override fun getAllLinkingFeedbacksWithFeatures(): Iterable<EntityLinkingFeatures> {
        return feedbackQueryService.getLinkingFeedbacks().map {
            val entities = dataLoader.getEntities(setOf(it.src, it.dst))
            EntityLinkingFeatures(
                    it,
                    matcher.extractFeatures(
                            matcher.extractProperties(entities.getValue(it.src)),
                            matcher.extractProperties(entities.getValue(it.dst))))
        }
    }

    @PostMapping(path = [LinkingFeedbackApi.FEEDBACK + LinkingFeedbackApi.FEATURES])
    override fun getLinkingFeedbackWithFeatures(
            @RequestBody entityPair: Pair<EntityDataKey, EntityDataKey>): EntityLinkingFeatures? {
        val feedback = checkNotNull(feedbackQueryService.getLinkingFeedback(entityPair))
        { "Linking feedback for entities ${entityPair.first} - ${entityPair.second} does not exist" }

        val entities = dataLoader.getEntities(setOf(feedback.src, feedback.dst))
        return EntityLinkingFeatures(
                feedback,
                matcher.extractFeatures(
                        matcher.extractProperties(entities.getValue(feedback.src)),
                        matcher.extractProperties(entities.getValue(feedback.dst))))
    }


    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}