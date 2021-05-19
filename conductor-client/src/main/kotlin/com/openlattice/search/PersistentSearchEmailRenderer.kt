package com.openlattice.search

import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.mail.RenderableEmailRequest
import com.openlattice.search.renderers.AlprAlertEmailRenderer
import com.openlattice.search.renderers.AlprHotlistEmailRenderer
import com.openlattice.search.renderers.BHRAlertEmailRenderer
import com.openlattice.search.renderers.CAREIssueAlertEmailRenderer
import com.openlattice.search.renderers.CodexAlertEmailRenderer
import com.openlattice.search.renderers.ReentryTaskAlertEmailRenderer
import com.openlattice.search.requests.PersistentSearch
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger(PersistentSearchEmailRenderer::class.java)

class PersistentSearchEmailRenderer {

    companion object {

        fun renderEmail(
                persistentSearch: PersistentSearch,
                entity: Map<FullQualifiedName, Set<Any>>,
                userEmail: String,
                neighbors: List<NeighborEntityDetails>,
                dependencies: PersistentSearchMessengerTaskDependencies
        ): RenderableEmailRequest? {

            return when (persistentSearch.type) {
                PersistentSearchNotificationType.ALPR_ALERT -> AlprAlertEmailRenderer.renderEmail(persistentSearch, entity, userEmail, neighbors, dependencies.mapboxToken)
                PersistentSearchNotificationType.ALPR_HOTLIST_ALERT -> AlprHotlistEmailRenderer.renderEmail(persistentSearch, entity, userEmail, dependencies.mapboxToken)
                PersistentSearchNotificationType.BHR_ALERT -> BHRAlertEmailRenderer.renderEmail(persistentSearch, entity, userEmail, neighbors)
                PersistentSearchNotificationType.CODEX_ALERT -> CodexAlertEmailRenderer.renderEmail(persistentSearch, entity, userEmail, neighbors)
                PersistentSearchNotificationType.CARE_ISSUE_ALERT -> CAREIssueAlertEmailRenderer.renderEmail(persistentSearch, entity, userEmail, neighbors)
                PersistentSearchNotificationType.REENTRY_TASK_ALERT -> ReentryTaskAlertEmailRenderer.renderEmail(persistentSearch, entity, userEmail, neighbors)

                else -> {
                    logger.error("Unable to render email for type {}", persistentSearch.type)
                    null
                }
            }
        }
    }
}
