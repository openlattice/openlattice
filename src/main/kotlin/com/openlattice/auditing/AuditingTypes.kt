package com.openlattice.auditing

import com.google.common.collect.LinkedHashMultimap
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.datastore.exceptions.ResourceNotFoundException
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*

/**
 *
 *
 */
class AuditingTypes(
        val edm: EdmManager,
        private val auditingConfiguration: AuditingConfiguration
) {
    companion object {
        private val logger = LoggerFactory.getLogger(AuditingTypes::class.java)
    }

    val entityType: EntityType
    val auditingEntityTypeId: UUID
    val propertyTypes: MutableMap<UUID, PropertyType> = mutableMapOf()
    val propertyTypeIds: MutableMap<AuditProperty, UUID> = mutableMapOf()

    init {
        val entityTypeFqn = FullQualifiedName(auditingConfiguration.entityTypeFqn)
        val maybeEntityTypeId = Optional.ofNullable(edm.getTypeAclKey(entityTypeFqn))
        val properties = linkedSetOf(getKeyProperties(auditingConfiguration, edm))


        entityType = maybeEntityTypeId
                .map { edm.getEntityType(it) }
                .orElseGet {
                    val newEntityType = EntityType(
                            entityTypeFqn, "Audit Entity Type",
                            "This the default created audit entity type.",
                            mutableSetOf(),
                            properties,
                            properties,
                            LinkedHashMultimap.create(),
                            Optional.empty(),
                            Optional.of(SecurableObjectType.EntityType)
                    )
                    edm.createEntityType(newEntityType)
                    edm.getEntityType(entityTypeFqn)
                }

        auditingEntityTypeId = entityType.id

        auditingConfiguration.fqns.forEach {
            val propertyType = edm.getPropertyType(FullQualifiedName(it.value)) //NPE if property type does not exist.
            propertyTypes[propertyType.id] = propertyType
            propertyTypeIds[it.key] = propertyType.id
        }

        val allPropertyTypes = edm.getPropertyTypesAsMap(entityType.properties)


        check(allPropertyTypes.keys.containsAll(propertyTypes.keys)) {
            val msg = "Auditing configuration specified the following property types not present in entity set: " +
                    (propertyTypes - allPropertyTypes)
            logger.error(msg)
            return@check msg
        }
    }

    private fun getKeyProperties(auditingConfiguration: AuditingConfiguration, edm: EdmManager): UUID {
        val idProperty = FullQualifiedName(auditingConfiguration.fqns[AuditProperty.ID]!!)
        val idPropertyType = PropertyType(
                idProperty,
                "The default id property",
                Optional.empty(),
                setOf(),
                EdmPrimitiveTypeKind.Guid
        )
        edm.createPropertyTypeIfNotExists(idPropertyType)
        return idPropertyType.id
    }

    fun getPropertyTypeId(auditProperty: AuditProperty): UUID {
        return if (auditingConfiguration.fqns.keys.contains(auditProperty)) {
            return propertyTypeIds[auditProperty]!!
        } else {
            throw ResourceNotFoundException("Audit property $auditProperty is not configured.")
        }
    }
}