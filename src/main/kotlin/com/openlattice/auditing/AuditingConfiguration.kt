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
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

private val logger = LoggerFactory.getLogger(AuditingConfiguration::class.java)

@ReloadableConfiguration(uri = "auditing.yaml")
data class AuditingConfiguration(
        val entityTypeFqn: FullQualifiedName,
        val fqns: Map<AuditProperty, FullQualifiedName>
) {
    private lateinit var auditingEntityTypeId: UUID
    val propertyTypes: MutableMap<UUID, PropertyType> = mutableMapOf()
    val propertyTypeIds: MutableMap<AuditProperty, UUID> = mutableMapOf()

    fun getAuditingEntityTypeId(): UUID {
        return auditingEntityTypeId
    }

    @Inject
    fun initPropertyTypes(edm: EdmManager) {
        val entityType = edm.getEntityType(entityTypeFqn)
        val allPropertyTypes = edm.getPropertyTypesAsMap(entityType.properties)

        auditingEntityTypeId = entityType.id

        fqns.forEach {
            val propertyType = edm.getPropertyType(it.value) //NPE if property type does not exist.
            propertyTypes[propertyType.id] = propertyType
            propertyTypeIds[it.key] = propertyType.id
        }
        check(allPropertyTypes.keys.containsAll(propertyTypes.keys)) {
            val msg = "Auditing configuration specified the following property types not present in entity set: " +
                    (propertyTypes - allPropertyTypes)
            logger.error(msg)
            return@check msg
        }
    }

    fun getPropertyTypeId(auditProperty: AuditProperty): UUID {
        return if (fqns.keys.contains(auditProperty)) {
            return propertyTypeIds[auditProperty]!!
        } else {
            throw ResourceNotFoundException("Audit property $auditProperty is not configured.")
        }
    }
}