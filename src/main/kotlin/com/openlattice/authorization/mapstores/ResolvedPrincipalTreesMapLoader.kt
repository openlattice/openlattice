package com.openlattice.authorization.mapstores

import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organizations.SecurablePrincipalList
import org.apache.commons.lang3.NotImplementedException
import org.apache.commons.lang3.RandomStringUtils
import java.util.*

/**
 * In memory cache of resolved principal trees
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ResolvedPrincipalTreesMapLoader : TestableSelfRegisteringMapStore<String, SecurablePrincipalList> {
    override fun getMapName(): String {
        return HazelcastMap.RESOLVED_PRINCIPAL_TREES.name
    }

    override fun loadAllKeys(): Iterable<String>? {
        return null
    }

    override fun getTable(): String {
        return ""
    }

    override fun generateTestValue(): SecurablePrincipalList {
        return SecurablePrincipalList(
                mutableListOf(
                        SecurablePrincipal(
                                Optional.empty(),
                                TestDataFactory.userPrincipal(),
                                "foobar",
                                Optional.empty()
                        )
                )
        )
    }

    override fun store(key: String, value: SecurablePrincipalList) {
        throw NotImplementedException("This is a read only map loader.")
    }

    override fun generateTestKey(): String {
        return RandomStringUtils.random(10)
    }

    override fun deleteAll(keys: Collection<String>) {
        throw NotImplementedException("This is a read only map loader.")
    }

    override fun storeAll(map: Map<String, SecurablePrincipalList>) {
        throw NotImplementedException("This is a read only map loader.")
    }

    override fun getMapConfig(): MapConfig {
        return MapConfig(mapName)
                .setMaxIdleSeconds(300)
                .setMapStoreConfig(mapStoreConfig)
    }

    override fun getMapStoreConfig(): MapStoreConfig {
        return MapStoreConfig()
                .setEnabled(false)
                .setImplementation(this)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
    }

    override fun loadAll(keys: MutableCollection<String>): Map<String, SecurablePrincipalList> {
        throw NotImplementedException("This is a read only a cache.")
    }

    override fun load(principalId: String): SecurablePrincipalList? {
        throw NotImplementedException("This is a read only a cache.")
    }


    override fun delete(key: String) {
        throw NotImplementedException("This is a read only a cache.")
    }

}