package com.openlattice.linking.controllers

import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
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
        if(feedback.src.entityKeyId == feedback.dst.entityKeyId) {
            logger.error("You cannot ")
            return false
        }

        feedbackQueryService.addLinkingFeedback(feedback)
        logger.info("Linking feedback submitted for entities: ${feedback.src} - ${feedback.dst}, linked = " +
                "${feedback.linked}")
        return true
    }

    @GetMapping(path = [LinkingFeedbackApi.FEEDBACK + LinkingFeedbackApi.ALL])
    override fun getLinkingFeedbacks(): Iterable<EntityLinkingFeedback> {
        return feedbackQueryService.getLinkingFeedbacks()
    }

    @GetMapping(path = [LinkingFeedbackApi.FEEDBACK + LinkingFeedbackApi.ALL + LinkingFeedbackApi.FEATURES])
    override fun getLinkingFeedbacksWithFeatures(): Iterable<EntityLinkingFeatures> {
        return feedbackQueryService.getLinkingFeedbacks().map {
            val entities = dataLoader.getEntities(setOf(it.src, it.dst))
            EntityLinkingFeatures(
                    it,
                    matcher.extractFeatures(
                            matcher.extractProperties(entities.getValue(it.src)),
                            matcher.extractProperties(entities.getValue(it.dst))))
        }
    }


    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}