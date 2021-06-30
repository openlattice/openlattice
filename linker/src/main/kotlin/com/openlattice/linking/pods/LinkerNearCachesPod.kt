package com.openlattice.linking.pods


import com.hazelcast.config.*
import com.openlattice.hazelcast.HazelcastMap
import org.springframework.context.annotation.Bean


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class LinkerNearCachesPod {
    @Bean
    fun entityTypesNearCache(): NearCacheConfig {
        val evictionConfig = EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT)
                .setSize(8192)
        return NearCacheConfig(HazelcastMap.ENTITY_TYPES.name)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setInvalidateOnChange(true)
                .setTimeToLiveSeconds(120)
                .setEvictionConfig(evictionConfig)
    }

    @Bean
    fun propertyTypesNearCache(): NearCacheConfig {
        val evictionConfig = EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT)
                .setSize(8192)
        return NearCacheConfig(HazelcastMap.PROPERTY_TYPES.name)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setInvalidateOnChange(true)
                .setTimeToLiveSeconds(120)
                .setEvictionConfig(evictionConfig)
    }

    @Bean
    fun entitySetsNearCache(): NearCacheConfig {
        val evictionConfig = EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT)
                .setSize(8192)
        return NearCacheConfig(HazelcastMap.ENTITY_SETS.name)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setInvalidateOnChange(true)
                .setTimeToLiveSeconds(120)
                .setEvictionConfig(evictionConfig)
    }
}