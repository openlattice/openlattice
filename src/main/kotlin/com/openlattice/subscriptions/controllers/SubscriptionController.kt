
package com.openlattice.subscriptions.controllers

import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.data.EntityDataKey
import com.openlattice.subscriptions.SubscriptionApi
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import javax.inject.Inject

@RestController
@RequestMapping(SubscriptionApi.CONTROLLER)
class SubscriptionController
@Inject
constructor(
        private val authorizationManager: AuthorizationManager
) : SubscriptionApi, AuthorizingComponent {

    override fun addSubscription(entity: EntityDataKey): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun updateSubscription(entity: EntityDataKey): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun deleteSubscription(entity: EntityDataKey): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getAllSubscriptions(): Iterable<Int> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}

