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

package com.openlattice.auditing

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import com.openlattice.datastore.exceptions.ResourceNotFoundException
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EdmService
import com.openlattice.edm.type.PropertyType
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*
import javax.inject.Inject

/**
 * Configuration class for auditing.
 */
@ReloadableConfiguration(uri = "auditing.yaml")
data class AuditingConfiguration(
        @JsonProperty("entity-type") val entityTypeFqn: String,
        @JsonProperty("fqns") val fqns: Map<AuditProperty, String>
)