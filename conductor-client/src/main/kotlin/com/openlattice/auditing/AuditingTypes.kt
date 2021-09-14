package com.openlattice.auditing

import com.openlattice.controllers.exceptions.ResourceNotFoundException
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.type.AssociationType
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.locks.ReentrantLock

/**
 *
 *
 */
@SuppressFBWarnings(value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"], justification = "Ignore internal kotlin redundant nullchecks")
class AuditingTypes(
        private val edm: EdmManager,
        private val auditingConfiguration: AuditingConfiguration
) {
    companion object {
        private val logger = LoggerFactory.getLogger(AuditingTypes::class.java)
    }

    lateinit var entityType: EntityType
    lateinit var edgeEntityType: AssociationType
    lateinit var auditingEntityTypeId: UUID
    lateinit var auditingEdgeEntityTypeId: UUID

    val propertyTypes: MutableMap<UUID, PropertyType> = mutableMapOf()
    val propertyTypeIds: MutableMap<AuditProperty, UUID> = mutableMapOf()
    private val lock = ReentrantLock()
    private var initialized = false

    fun intialize() {
        if (!initialized) {
            if (lock.tryLock()) {
                try {
                    val entityTypeFqn = FullQualifiedName(auditingConfiguration.entityTypeFqn)
                    val edgeEntityTypeFqn = FullQualifiedName(auditingConfiguration.edgeEntityTypeFqn)

                    entityType = edm.getEntityType(entityTypeFqn)
                    edgeEntityType = edm.getAssociationType(edgeEntityTypeFqn)

                    auditingEntityTypeId = entityType.id
                    auditingEdgeEntityTypeId = edgeEntityType.associationEntityType.id

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
        if (auditingConfiguration.fqns.keys.contains(auditProperty)) {
            return propertyTypeIds[auditProperty]!!
        } else {
            throw ResourceNotFoundException("Audit property $auditProperty is not configured.")
        }
    }

    fun enabled(): Boolean {
        return auditingConfiguration.enabled
    }

    fun isAuditingInitialized(): Boolean {
        if (!initialized) {
            intialize()
        }
        return initialized
    }
}