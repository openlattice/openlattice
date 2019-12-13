package com.openlattice.authorization.mapstores

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSortedSet
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore
import com.openlattice.authorization.*
import com.openlattice.authorization.mapstores.PrincipalMapstore.PRINCIPAL_INDEX
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
    private lateinit var aclKeys: IMap<String, UUID>
    private lateinit var principals: IMap<AclKey, SecurablePrincipal>
    private lateinit var principalTrees: IMap<AclKey, AclKeySet>
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
        return keys
                .associateWith(this::load)
                .filterValues { it != null } as Map<String, NavigableSet<Principal>>
    }

    override fun load(principalId: String): NavigableSet<Principal>? {
        val sp = getPrincipal(principalId) ?: return null
        val securablePrincipals = getAllPrincipals(sp) ?: return null

        val currentPrincipals: NavigableSet<Principal> = TreeSet()
        currentPrincipals.add(sp.principal)
        securablePrincipals.stream()
                .map(SecurablePrincipal::getPrincipal)
                .forEach { currentPrincipals.add(it) }
        return currentPrincipals
    }

    private fun getLayer(aclKeys: Set<AclKey>): AclKeySet? {
        return principalTrees.aggregate(
                AccumulatingReadAggregator<AclKey>(),
                Predicates.`in`("__key", *aclKeys.toTypedArray()) as Predicate<AclKey, AclKeySet>
        )
    }

    private fun getAllPrincipals(sp: SecurablePrincipal): Collection<SecurablePrincipal>? {
        val roles = getLayer(setOf(sp.aclKey)) ?: return ImmutableList.of()
        var nextLayer: Set<AclKey> = roles

        while (nextLayer.isNotEmpty()) {
            nextLayer = getLayer(nextLayer) ?: setOf()
            roles.addAll(nextLayer)
        }

        return principals.aggregate(
                SecurablePrincipalAccumulator(),
                Predicates.`in`("__key", *roles.toTypedArray()) as Predicate<AclKey, SecurablePrincipal>)

    }


    private fun getPrincipal(principalId: String): SecurablePrincipal? {
//        val principal = aclKeys.aggregate(
//                ReadAggregator<String, UUID>(),
//                Predicates.equal("__key", principalId) as Predicate<String, UUID>
//        )

        //This will always only be called on users. If it needs to be fixed for other types than an additional index
        //will have to be added.
        return principals.aggregate(
                ReadAggregator<AclKey, SecurablePrincipal>(),
                Predicates.equal(
                        PRINCIPAL_INDEX, Principals.getUserPrincipal(principalId)
                ) as Predicate<AclKey, SecurablePrincipal>
        )

    }

    override fun delete(key: String) {
        throw NotImplementedException("This is a read only map loader.")
    }

    fun initSpm(spm: SecurePrincipalsManager) {
        this.spm = spm
    }

    fun initPrincipalsMapstore(hazelcastInstance: HazelcastInstance) {
        this.principals = hazelcastInstance.getMap(HazelcastMap.SECURABLE_PRINCIPALS.name)
        this.principalTrees = hazelcastInstance.getMap(HazelcastMap.PRINCIPAL_TREES.name)
    }

}