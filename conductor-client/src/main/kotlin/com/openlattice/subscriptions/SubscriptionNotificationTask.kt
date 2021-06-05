package com.openlattice.subscriptions

import com.openlattice.authorization.Permission
import com.openlattice.authorization.Principals
import com.openlattice.graph.NeighborhoodQuery
import com.openlattice.mail.RenderableEmailRequest
import com.openlattice.notifications.sms.SubscriptionNotification
import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.HazelcastTaskDependencies
import java.util.*
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class SubscriptionNotificationTask : HazelcastFixedRateTask<SubscriptionNotificationDependencies>, HazelcastTaskDependencies {

    companion object {
        const val DEFAULT_MESSAGE = "One of your subscriptions was involved in an event."
    }

    override fun getInitialDelay(): Long {
        return 30000
    }

    override fun getPeriod(): Long {
        return 30000
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun runTask() {
    }

    private fun doTask() {
        val dependencies = getDependency()
        dependencies.subscriptionService.getAllSubscriptions().forEach { (principal, subscriptionContact) ->
            val allEntitySetIds = getAllEntitySetIds(subscriptionContact.query)
            val authorizedPropertyTypes = dependencies.authorizationHelper.getAuthorizedPropertiesOnEntitySets(
                    allEntitySetIds,
                    EnumSet.of(Permission.READ),
                    Principals.getUserPrincipals(principal.id)
            )
            val propertyTypes = authorizedPropertyTypes.values.flatMap { it.values }.associateBy { it.id }
            val neighborhood = dependencies.graphQueryService.submitQuery(
                    subscriptionContact.query,
                    propertyTypes,
                    authorizedPropertyTypes,
                    Optional.of(LastWriteRangeFilter(subscriptionContact.lastNotify))
            )

            //If there are notifications fire the notifications.
            if (neighborhood.entities.isNotEmpty()) {
                subscriptionContact.contact.forEach { (contactType, contact) ->
                    when (contactType) {
                        SubscriptionContactType.PHONE -> {
                            dependencies.twilioFeedQueue.put(
                                    SubscriptionNotification(
                                            DEFAULT_MESSAGE, // TODO more specific message once this feature is ready
                                            contact
                                    )
                            )
                        }
                        SubscriptionContactType.EMAIL -> {
                            val data = mutableMapOf<String, Any>()

                            dependencies.mailServiceClient.spool(
                                    RenderableEmailRequest(
                                            Optional.of("notifications@openlattice.com"),
                                            arrayOf(contact),
                                            Optional.empty(),
                                            Optional.empty(),
                                            "mail/templates/shared/CodexAlertTemplate.mustache",
                                            Optional.of(DEFAULT_MESSAGE),
                                            Optional.of(data),
                                            Optional.empty(),
                                            Optional.empty()
                                    )
                            )
                        }
                    }
                }
                dependencies.subscriptionService.markLastNotified(
                        neighborhood.entities.values.flatMap { it.keys }.toSet(), principal
                )
            }
        }
    }

    private fun getAllEntitySetIds(query: NeighborhoodQuery): Set<UUID> {
        val dependencies = getDependency()
        return dependencies.graphQueryService
                .getEntitySetForIds(query.ids.flatMap { it.value.orElse(emptySet()) }.toSet())
                .values.toSet() +
                (query.srcSelections + query.dstSelections).flatMap { selection ->
                    dependencies.graphQueryService.getEntitySets(selection.entityTypeIds) +
                            dependencies.graphQueryService.getEntitySets(selection.associationTypeIds)
                }

    }

    override fun getName(): String {
        return "subscription_notifications_task"
    }

    override fun getDependenciesClass(): Class<out SubscriptionNotificationDependencies> {
        return SubscriptionNotificationDependencies::class.java
    }
}