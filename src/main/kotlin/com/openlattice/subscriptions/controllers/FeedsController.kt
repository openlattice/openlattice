package com.openlattice.subscriptions.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.auditing.AuditEventType
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.authorization.EdmAuthorizationHelper
import com.openlattice.authorization.Principals
import com.openlattice.data.EntityDataKey
import com.openlattice.graph.GraphApi
import com.openlattice.graph.GraphQueryService
import com.openlattice.subscriptions.FeedEntry
import com.openlattice.subscriptions.FeedsApi
import com.openlattice.subscriptions.SubscriptionService
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject

@RestController
@RequestMapping(FeedsApi.CONTROLLER)
class FeedsController
@Inject
constructor(
        private val authorizationManager: AuthorizationManager,
        private val authzHelper: EdmAuthorizationHelper,
        private val subscriptionService: SubscriptionService,
        private val graphApi: GraphApi,
        private val graphQueryService: GraphQueryService
) : FeedsApi, AuthorizingComponent {

    @Timed
    @RequestMapping(path = ["", "/"], method = [RequestMethod.GET])
    override fun getLatestFeed(): Iterator<FeedEntry> {

        //Ret EDK + eventType + EDKs + Principal
        val allSubscriptions = subscriptionService.getAllSubscriptions(Principals.getCurrentUser())

//        authzHelper.getAuthorizedPropertiesOnEntitySets( authorizedEntitySetIds, READ_PERMISSION, Principals.getCurrentPrincipals() );
//        authzHelper.getAuthorizedPropertiesOnEntitySets(entitySetIds, EnumSet.of( Permission.READ ), setOf( organizationPrincipal.getPrincipal() ) );

//        allSubscriptions.forEach { sub ->
//            graphQueryService.submitQuery(sub, )
//        }

        return allSubscriptions.map { return@map FeedEntry(EntityDataKey(UUID.randomUUID(), UUID.randomUUID()), AuditEventType.CREATE_ENTITY_TYPE, setOf(), Principals.getCurrentUser()) }.listIterator()
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}

