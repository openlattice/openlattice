package com.openlattice.search

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.scheduledexecutor.IScheduledFuture
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.mail.MailServiceClient
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.zaxxer.hikari.HikariDataSource

class PersistentSearchMessengerHelpers {

    companion object {

        @Transient
        @JvmStatic
        lateinit var syncFuture : IScheduledFuture<*>

        @Transient
        @JvmStatic
        var initialized = false

        @Transient
        @JvmStatic
        lateinit var hds: HikariDataSource

        @Transient
        @JvmStatic
        lateinit var hazelcastInstance: HazelcastInstance

        @Transient
        @JvmStatic
        lateinit var principalsManager: SecurePrincipalsManager

        @Transient
        @JvmStatic
        lateinit var authorizationManager: AuthorizationManager

        @Transient
        @JvmStatic
        lateinit var searchService: SearchService

        @Transient
        @JvmStatic
        lateinit var mailServiceClient: MailServiceClient

        @Transient
        @JvmStatic
        lateinit var mapboxToken: String
    }
}