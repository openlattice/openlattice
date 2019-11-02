package com.openlattice.organizations

import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration

@ReloadableConfiguration(uri = "pg_hba_config.yaml")
data class OrganizationExternalDatabaseConfiguration(
        val path: String,
        val fileName: String,
        val authMethod: String
) {
}