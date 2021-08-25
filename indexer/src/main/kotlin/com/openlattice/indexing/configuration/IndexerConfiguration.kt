/*
 * Copyright (C) 2019. OpenLattice, Inc.
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

package com.openlattice.indexing.configuration

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.Configuration
import com.kryptnostic.rhizome.configuration.ConfigurationKey
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import com.openlattice.conductor.rpc.SearchConfiguration


/**
 * In
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

private const val SEARCH = "search"
private const val ERROR_REPORTING_EMAIL = "error-reporting-email"
private const val BACKGROUND_INDEXING_ENABLED = "background-indexing-enabled"
private const val BACKGROUND_LINKING_INDEXING_ENABLED = "background-linking-indexing-enabled"
private const val BACKGROUND_EXPIRED_DATA_DELETION_ENABLED = "background-expired-data-deletion-enabled"
private const val BACKGROUND_EXTERNAL_DATABASE_SYNCING_ENABLED = "background-external-database-syncing-enabled"
private const val BACKGROUND_DELETION_ENABLED = "background-deletion-enabled"

@ReloadableConfiguration(uri = "indexer.yaml")
data class IndexerConfiguration(
        @JsonProperty(SEARCH) val searchConfiguration: SearchConfiguration,
        @JsonProperty(ERROR_REPORTING_EMAIL) val errorReportingEmail: String,
        @JsonProperty(BACKGROUND_INDEXING_ENABLED) val backgroundIndexingEnabled: Boolean = true,
        @JsonProperty(BACKGROUND_LINKING_INDEXING_ENABLED) val backgroundLinkingIndexingEnabled: Boolean = true,
        @JsonProperty(BACKGROUND_EXPIRED_DATA_DELETION_ENABLED) val backgroundExpiredDataDeletionEnabled: Boolean = true,
        @JsonProperty(BACKGROUND_EXTERNAL_DATABASE_SYNCING_ENABLED) val backgroundExternalDatabaseSyncingEnabled: Boolean = true,
        @JsonProperty(BACKGROUND_DELETION_ENABLED) val backgroundDeletionEnabled: Boolean = true,
        @JsonProperty("parallelism") val parallelism : Int = Runtime.getRuntime().availableProcessors()
) : Configuration {
    companion object {
        @JvmStatic
        val configKey = SimpleConfigurationKey("indexer.yaml")
    }

    @JsonIgnore
    override fun getKey(): ConfigurationKey {
        return configKey
    }
}