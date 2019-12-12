package com.openlattice.authorization.mapstores

import com.google.common.collect.ImmutableSortedSet
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore
import com.openlattice.authorization.Principal
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organizations.roles.SecurePrincipalsManager
import org.apache.commons.lang3.NotImplementedException
import org.apache.commons.lang3.RandomStringUtils
import java.util.*

/**
 * In memory cache of resolved principal trees
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ResolvedPrincipalTreesMapLoader : TestableSelfRegisteringMapStore<String, NavigableSet<Principal>> {
    private lateinit var spm: SecurePrincipalsManager
    private lateinit var principals: IMap<String, SecurablePrincipal>

    override fun getMapName(): String {
        return HazelcastMap.RESOLVED_PRINCIPAL_TREES.name
    }

    override fun loadAllKeys(): Iterable<String>? {
        return null
    }

    override fun getTable(): String {
        return ""
    }

    override fun generateTestValue(): NavigableSet<Principal> {
        return ImmutableSortedSet.of(TestDataFactory.userPrincipal())
    }

    override fun store(key: String, value: NavigableSet<Principal>) {
        throw NotImplementedException("This is a read only map loader.")
    }

    override fun generateTestKey(): String {
        return RandomStringUtils.random(10)
    }

    override fun deleteAll(keys: Collection<String>) {
        throw NotImplementedException("This is a read only map loader.")
    }

    override fun storeAll(map: Map<String, NavigableSet<Principal>>) {
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

    override fun loadAll(keys: MutableCollection<String>): Map<String, NavigableSet<Principal>> {
        return keys.associateWith { load(it) }
    }

    override fun load(principalId: String): NavigableSet<Principal> {
        val sp = principals[principalId] ?: return ImmutableSortedSet.of()
        val securablePrincipals = spm.getAllPrincipals(sp) ?: return ImmutableSortedSet.of()

        val currentPrincipals: NavigableSet<Principal> = TreeSet()
        currentPrincipals.add(sp.principal)
        securablePrincipals.stream()
                .map(SecurablePrincipal::getPrincipal)
                .forEach { currentPrincipals.add(it) }
        return currentPrincipals
    }

    override fun delete(key: String) {
        throw NotImplementedException("This is a read only map loader.")
    }

    fun initSpm(spm: SecurePrincipalsManager) {
        this.spm = spm;
    }

    fun initPrincipalsMapstore(hazelcastInstance: HazelcastInstance) {
        this.principals = hazelcastInstance.getMap(HazelcastMap.SECURABLE_PRINCIPALS.name)
    }

}