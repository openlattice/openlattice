package com.openlattice.authorization.mapstores

import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organizations.roles.SecurePrincipalsManager
import org.apache.commons.lang3.NotImplementedException
import org.apache.commons.lang3.RandomStringUtils
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class SecurablePrincipalsMapLoader : TestableSelfRegisteringMapStore<String, SecurablePrincipal> {
    private lateinit var spm: SecurePrincipalsManager

    override fun getMapName(): String {
        return HazelcastMap.SECURABLE_PRINCIPALS.name
    }

    override fun loadAllKeys(): Iterable<String>? {
        return null
    }

    override fun getTable(): String {
        return ""
    }

    override fun generateTestValue(): SecurablePrincipal {
        return SecurablePrincipal(Optional.empty(), TestDataFactory.userPrincipal(), "foobar", Optional.empty())
    }

    override fun store(key: String, value: SecurablePrincipal) {
        throw NotImplementedException("This is a read only map loader.")
    }

    override fun generateTestKey(): String {
        return RandomStringUtils.random(10)
    }

    override fun deleteAll(keys: MutableCollection<String>?) {
        throw NotImplementedException("This is a read only map loader.")
    }

    override fun storeAll(map: MutableMap<String, SecurablePrincipal>?) {
        throw NotImplementedException("This is a read only map loader.")
    }

    override fun getMapConfig(): MapConfig {
        return MapConfig(mapName)
                .setMaxIdleSeconds(300)
                .setMapStoreConfig(mapStoreConfig)
    }

    override fun getMapStoreConfig(): MapStoreConfig {
        return MapStoreConfig()
                .setEnabled(true)
                .setImplementation(this)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
    }

    override fun loadAll(keys: MutableCollection<String>): Map<String, SecurablePrincipal> {
        return keys.associateWith { spm.getPrincipal(it) }
    }

    override fun load(key: String): SecurablePrincipal {
        return spm.getPrincipal(key)
    }

    override fun delete(key: String) {
        throw NotImplementedException("This is a read only map loader.")
    }

    fun initSpm(spm: SecurePrincipalsManager) {
        this.spm = spm
    }

}