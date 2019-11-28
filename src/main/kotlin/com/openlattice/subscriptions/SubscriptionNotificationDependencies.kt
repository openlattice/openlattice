package com.openlattice.subscriptions

import com.hazelcast.core.IQueue
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.EdmAuthorizationHelper
import com.openlattice.codex.MessageRequest
import com.openlattice.graph.GraphQueryService
import com.openlattice.mail.MailServiceClient
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.tasks.HazelcastTaskDependencies
import com.zaxxer.hikari.HikariDataSource

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
        val graphQueryService: GraphQueryService,
        val twilioQueue: IQueue<MessageRequest>

) : HazelcastTaskDependencies {
    constructor(
            hds: HikariDataSource,
            principalsManager: SecurePrincipalsManager,
            authorizationManager: AuthorizationManager,
             subscriptionService: SubscriptionService,
            graphQueryService: GraphQueryService,
            authorizationHelper: EdmAuthorizationHelper,
            mailServiceClient: MailServiceClient,
            twilioQueue: IQueue<MessageRequest>
    ) : this(
            hds,
            principalsManager,
            authorizationManager,
            authorizationHelper,
            mailServiceClient,
            subscriptionService,
            graphQueryService,
            twilioQueue)
}