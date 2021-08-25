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

package com.openlattice.linking.controllers

import com.google.common.collect.Sets
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.data.EntityDataKey
import com.openlattice.data.storage.IndexingMetadataManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.linking.DataLoader
import com.openlattice.linking.EntityKeyPair
import com.openlattice.linking.EntityLinkingFeatures
import com.openlattice.linking.EntityLinkingFeedback
import com.openlattice.linking.FeedbackType
import com.openlattice.linking.LinkingFeedback
import com.openlattice.linking.LinkingFeedbackApi
import com.openlattice.linking.PostgresLinkingFeedbackService
import com.openlattice.linking.graph.PostgresLinkingQueryService
import com.openlattice.linking.matching.Matcher
import com.openlattice.linking.util.PersonMetric
import com.openlattice.linking.util.PersonProperties
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
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
        private val linkingQueryService: PostgresLinkingQueryService,
        private val dataManager: IndexingMetadataManager,
        private val entitySetManager: EntitySetManager
) : LinkingFeedbackApi, AuthorizingComponent {


    private val personPropertyIds = PersonProperties.FQNS.map { edm.getPropertyTypeId(it) }

    companion object {
        private val logger = LoggerFactory.getLogger(LinkingFeedbackController::class.java)
    }

    @PutMapping(path = [], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun addLinkingFeedback(@RequestBody feedback: LinkingFeedback): Int {
        require(feedback.link.isNotEmpty() && (feedback.link.size + feedback.unlink.size) >= 2) {
            "Cannot submit feedback for less than 2 entities or if no positively linking entity is provided."
        }

        val interSection = Sets.intersection(feedback.link, feedback.unlink)
        require(interSection.isEmpty()) {
            "Cannot submit feedback with entities $interSection being both linking and non-linking."
        }

        // ensure read access on linking entity set
        ensureReadAccess(AclKey(feedback.linkingEntityDataKey.entitySetId))

        // ensure read access on all entity sets involved and the properties used for linking
        // make sure, that all entitysets are part of the linking entity set and entity ids have the provided linking id
        linkingFeedbackCheck(
                feedback.link + feedback.unlink,
                feedback.linkingEntityDataKey)

        // add feedbacks
        val linkingEntities = feedback.link.toTypedArray()
        val nonLinkingEntities = feedback.unlink.toTypedArray()

        var positiveFeedbackCount = 0
        var negativeFeedbackCount = 0

        linkingEntities.forEachIndexed { index, linkingEntity ->
            // generate pairs between linking entities themselves
            positiveFeedbackCount += createLinkingFeedbackCombinations(
                    linkingEntity, linkingEntities, index + 1, true)

            // generate pairs between linking and non-linking entities
            negativeFeedbackCount += createLinkingFeedbackCombinations(
                    linkingEntity, nonLinkingEntities, 0, false)
        }

        // mark entities as need to be linked
        dataManager.markAsNeedsToBeLinked(feedback.link + feedback.unlink)


        logger.info("Submitted $positiveFeedbackCount positive and $negativeFeedbackCount negative feedback for " +
                "linking id ${feedback.linkingEntityDataKey.entityKeyId}")

        return positiveFeedbackCount + negativeFeedbackCount
    }

    private fun linkingFeedbackCheck(entityDataKeys: Set<EntityDataKey>, linkingEntityDataKey: EntityDataKey) {
        val linkingEntitySet = entitySetManager.getEntitySet(linkingEntityDataKey.entitySetId)!!
        val entityKeyIdsOfLinkingId = linkingQueryService.getEntityKeyIdsOfLinkingIds(
                setOf(linkingEntityDataKey.entityKeyId),
                linkingEntitySet.linkedEntitySets
        ).first().second

        entityDataKeys.forEach { entityDataKey ->
            require(linkingEntitySet.linkedEntitySets.contains(entityDataKey.entitySetId)) {
                "Feedback can only be submitted for entities contained by linking entity set"
            }
            require(entityKeyIdsOfLinkingId.contains(entityDataKey.entityKeyId)) {
                "Feedback can only be submitted for entities with same linking id"
            }

            ensureReadAccess(AclKey(entityDataKey.entitySetId))
            personPropertyIds.forEach { ensureReadAccess(AclKey(entityDataKey.entitySetId, it)) }
        }
    }

    private fun createLinkingFeedbackCombinations(
            entityDataKey: EntityDataKey, entityList: Array<EntityDataKey>, offset: Int, linked: Boolean): Int {
        (offset until entityList.size).forEach {
            val elf = EntityLinkingFeedback(EntityKeyPair(entityDataKey, entityList[it]), linked)

            feedbackService.addLinkingFeedback(elf)

            logger.info("Linking feedback submitted for entities: $entityDataKey - ${entityList[it]}, linked = $linked")
        }
        return entityList.size
    }

    @PostMapping(path = [LinkingFeedbackApi.ENTITY], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getLinkingFeedbackOnEntity(
            @RequestParam(value = LinkingFeedbackApi.FEEDBACK_TYPE, required = false) feedbackType: FeedbackType,
            @RequestBody entity: EntityDataKey): Iterable<EntityLinkingFeedback> {
        return feedbackService.getLinkingFeedbackOnEntity(feedbackType, entity)
    }

    @PostMapping(path = [LinkingFeedbackApi.FEATURES], produces = [MediaType.APPLICATION_JSON_VALUE])
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

    @GetMapping(path = [LinkingFeedbackApi.ALL], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getAllLinkingFeedback(): Iterable<EntityLinkingFeedback> {
        return feedbackService.getLinkingFeedback()
    }

    @GetMapping(
            path = [LinkingFeedbackApi.FEATURES + LinkingFeedbackApi.ALL],
            produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getAllLinkingFeedbackWithFeatures(): Iterable<EntityLinkingFeatures> {
        return feedbackService.getLinkingFeedback().map {
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

    @DeleteMapping(path = [], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun deleteLinkingFeedback(@RequestBody entityPair: EntityKeyPair): Int {
        ensureAdminAccess() // currently it's only used by tests
        val count = feedbackService.deleteLinkingFeedback(entityPair)
        // mark entities as need to be linked
        dataManager.markAsNeedsToBeLinked(setOf(entityPair.first, entityPair.second))
        return count
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}