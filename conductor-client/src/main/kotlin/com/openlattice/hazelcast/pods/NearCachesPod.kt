package com.openlattice.hazelcast.pods

import com.hazelcast.config.EvictionConfig
import com.hazelcast.config.EvictionPolicy
import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MaxSizePolicy
import com.hazelcast.config.NearCacheConfig
import com.openlattice.hazelcast.HazelcastMap
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

private const val PERMISSIONS_NEAR_CACHE_TTL = 300
/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
class NearCachesPod {
    @Bean
    fun authenticationPrincipalsCache(): NearCacheConfig {
        //The default settings here should be good enough (LRU, 10K size)
        //We cache local entries because in memory format is different from map (binary)
        return NearCacheConfig(HazelcastMap.SECURABLE_PRINCIPALS.name)
                .setInvalidateOnChange(true)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setMaxIdleSeconds(30)
    }

    @Bean
    fun authenticationResolvedPrincipalTreesCache(): NearCacheConfig {
        //The default settings here should be good enough (LRU, 10K size)
        //We cache local entries because in memory format is different from map (binary)
        return NearCacheConfig(HazelcastMap.RESOLVED_PRINCIPAL_TREES.name)
                .setInvalidateOnChange(true)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setMaxIdleSeconds(30)
    }

    @Bean
    fun permissionsNearCacheConfig(): NearCacheConfig? {
        val evictionConfig = EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT)
                .setSize(8192)
        return NearCacheConfig(HazelcastMap.PERMISSIONS.name)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setInvalidateOnChange(true)
                .setTimeToLiveSeconds(PERMISSIONS_NEAR_CACHE_TTL)
                .setEvictionConfig(evictionConfig)
    }
}