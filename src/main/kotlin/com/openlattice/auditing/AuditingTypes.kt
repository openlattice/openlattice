package com.openlattice.auditing

import com.openlattice.controllers.exceptions.ResourceNotFoundException
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.locks.ReentrantLock

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

    lateinit var entityType: EntityType
    lateinit var auditingEntityTypeId: UUID
    val propertyTypes: MutableMap<UUID, PropertyType> = mutableMapOf()
    val propertyTypeIds: MutableMap<AuditProperty, UUID> = mutableMapOf()
    private val lock = ReentrantLock()
    private var initialized = false

    fun intialize() {
        if (!initialized) {
            if (lock.tryLock()) {
                try {
                    val entityTypeFqn = FullQualifiedName(auditingConfiguration.entityTypeFqn)
                    entityType = edm.getEntityType(entityTypeFqn)

                    auditingEntityTypeId = entityType.id

                    auditingConfiguration.fqns.forEach {
                        val propertyType = edm.getPropertyType(
                                FullQualifiedName(it.value)
                        ) //NPE if property type does not exist.
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

                    initialized = true
                } catch (ex: Exception) {
                    logger.error("Unable to complete initialization.", ex)
                } finally {
                    lock.unlock()
                }
            }
        }
    }

    fun getPropertyTypeId(auditProperty: AuditProperty): UUID {
        return if (auditingConfiguration.fqns.keys.contains(auditProperty)) {
            return propertyTypeIds[auditProperty]!!
        } else {
            throw ResourceNotFoundException("Audit property $auditProperty is not configured.")
        }
    }

    fun isAuditingInitialized(): Boolean {
        if (!initialized) {
            intialize()
        }
        return initialized
    }
}