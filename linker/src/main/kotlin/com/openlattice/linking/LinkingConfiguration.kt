/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.linking

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.Configuration
import com.kryptnostic.rhizome.configuration.ConfigurationKey
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import com.openlattice.conductor.rpc.SearchConfiguration
import com.openlattice.linking.util.PersonProperties
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*


const val DEFAULT_BLOCK_SIZE = 10_000
private const val CONFIG_YAML_NAME = "linking.yaml"
private const val BLOCK_SIZE_FIELD = "block-size"
private const val BLACKLIST = "blacklist"
private const val ENTITY_TYPES_FIELD = "entity-types"
private const val WHITELIST = "whitelist"
private const val SEARCH_CONFIGURATION = "searchConfiguration"
private const val BATCH_SIZE = "batch-size"
private const val LOAD_SIZE = "load-size"
private val DEFAULT_ENTITY_TYPES = setOf(PersonProperties.PERSON_TYPE_FQN.fullQualifiedNameAsString)

/**
 * Configuration class for linking.
 */
@ReloadableConfiguration(uri = CONFIG_YAML_NAME)
data class LinkingConfiguration(
        @JsonProperty(SEARCH_CONFIGURATION) val searchConfiguration: SearchConfiguration,
        @JsonProperty(BLOCK_SIZE_FIELD) val blockSize: Int = DEFAULT_BLOCK_SIZE,
        @JsonProperty(WHITELIST) val whitelist: Optional<Set<UUID>>,
        @JsonProperty(BLACKLIST) val blacklist: Set<UUID> = setOf(),
        @JsonProperty(BATCH_SIZE) val batchSize: Int = 10,
        @JsonProperty(LOAD_SIZE) val loadSize: Int = 100,
        @JsonProperty(ENTITY_TYPES_FIELD) private val entityTypesFqns: Set<String> = DEFAULT_ENTITY_TYPES
) : Configuration {
    companion object {
        @JvmStatic
        private val configKey = SimpleConfigurationKey(CONFIG_YAML_NAME)
    }

    val entityTypes: Set<FullQualifiedName> = entityTypesFqns.map { FullQualifiedName(it) }.toSet()


    @JsonIgnore
    override fun getKey(): ConfigurationKey {
        return configKey
    }
}