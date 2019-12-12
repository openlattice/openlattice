package com.openlattice.hazelcast.pods

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.NearCacheConfig
import com.openlattice.hazelcast.HazelcastMap
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

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
        return NearCacheConfig(HazelcastMap.SECURABLE_PRINCIPALS.name + "_authentication")
                .setInvalidateOnChange(true)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setCacheLocalEntries(true)
                .setMaxIdleSeconds(30)
    }

    @Bean
    fun authenticationResolvedPrincipalTreesCache(): NearCacheConfig {
        //The default settings here should be good enough (LRU, 10K size)
        //We cache local entries because in memory format is different from map (binary)
        return NearCacheConfig(HazelcastMap.RESOLVED_PRINCIPAL_TREES.name + "_authentication")
                .setInvalidateOnChange(true)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setCacheLocalEntries(true)
                .setMaxIdleSeconds(30)
    }
}