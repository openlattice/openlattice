package com.openlattice.users

import com.hazelcast.core.HazelcastInstance
import com.openlattice.auth0.Auth0TokenProvider
import com.openlattice.authorization.DbCredentialService
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import java.util.concurrent.locks.ReentrantLock

class Auth0SyncHelpers {
    companion object {
        @Transient
        @JvmStatic
        var initialized = false

        @Transient
        @JvmStatic
        lateinit var hazelcastInstance: HazelcastInstance

        @Transient
        @JvmStatic
         lateinit var spm: SecurePrincipalsManager

        @Transient
        @JvmStatic
         lateinit var organizationService: HazelcastOrganizationService

        @Transient
        @JvmStatic
         lateinit var dbCredentialService: DbCredentialService

        @Transient
        @JvmStatic
         lateinit var auth0TokenProvider: Auth0TokenProvider
    }
}