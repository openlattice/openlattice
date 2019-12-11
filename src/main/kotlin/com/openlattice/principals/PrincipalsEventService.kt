package com.openlattice.principals

import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.Principals
import com.openlattice.hazelcast.HazelcastTopic

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PrincipalsEventService(
        eventBus : EventBus
)  {
    init {
        eventBus.register(this)
    }

    @Subscribe
    fun invalidPrincipalTree( event : PrincipalTreeInvalidatedEvent) {
        Principals.invalidatePrincipalCache(event.principalId)
    }
}