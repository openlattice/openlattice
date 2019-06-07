package com.openlattice.subscriptions

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.EdmAuthorizationHelper
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.GraphQueryService
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mail.MailServiceClient
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.search.SearchService
import com.openlattice.tasks.HazelcastTaskDependencies
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class SubscriptionNotificationDependencies(
        val hds: HikariDataSource,
        val principalsManager: SecurePrincipalsManager,
        val authorizationManager: AuthorizationManager,
        val authorizationHelper: EdmAuthorizationHelper,
        val mailServiceClient: MailServiceClient,
        val subscriptionService: SubscriptionService,
        val graphQueryService: GraphQueryService

) : HazelcastTaskDependencies {
    constructor(
            hds: HikariDataSource,
            principalsManager: SecurePrincipalsManager,
            authorizationManager: AuthorizationManager,
             subscriptionService: SubscriptionService,
            graphQueryService: GraphQueryService,
            authorizationHelper: EdmAuthorizationHelper,
            mailServiceClient: MailServiceClient
    ) : this(
            hds,
            principalsManager,
            authorizationManager,
            authorizationHelper,
            mailServiceClient,
            subscriptionService,
            graphQueryService            )
}