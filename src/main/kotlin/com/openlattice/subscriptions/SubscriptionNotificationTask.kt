package com.openlattice.subscriptions

import com.openlattice.analysis.requests.WrittenTwoWeeksFilter
import com.openlattice.authorization.Permission
import com.openlattice.authorization.Principals
import com.openlattice.graph.NeighborhoodQuery
import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.HazelcastTaskDependencies
import java.util.*
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class SubscriptionNotificationTask : HazelcastFixedRateTask<SubscriptionNotificationDependencies>, HazelcastTaskDependencies {
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
        val dependencies = getDependency()
        dependencies.subscriptionService.getAllSubscriptions().forEach { (principal, subscriptionContact) ->
            val allEntitySetIds = getAllEntitySetIds(subscriptionContact.subscription)
            val authorizedPropertyTypes = dependencies.authorizationHelper.getAuthorizedPropertiesOnEntitySets(
                    allEntitySetIds,
                    EnumSet.of(Permission.READ),
                    Principals.getUserPrincipals(principal.id)
            )
            val propertyTypes = authorizedPropertyTypes.values.flatMap { it.values }.associateBy { it.id }
            val neighborhood = dependencies.graphQueryService.submitQuery(
                    subscriptionContact.subscription,
                    propertyTypes,
                    authorizedPropertyTypes,
                    Optional.of(LastWriteRangeFilter(subscriptionContact.lastNotify))
            )

            //If there are notifications fire the notifications.
            if( neighborhood.entities.isNotEmpty() ) {
                subscriptionContact.contact.forEach { contactType, contact ->

                }
            }
        }
    }

    private fun getAllEntitySetIds(query: NeighborhoodQuery): Set<UUID> {
        val dependencies = getDependency()
        return query.ids +
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