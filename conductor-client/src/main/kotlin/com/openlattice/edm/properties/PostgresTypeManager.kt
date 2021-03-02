package com.openlattice.edm.properties

import com.dataloom.streams.StreamUtil
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Lists
import com.google.common.collect.Queues
import com.google.common.collect.Sets
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.edm.schemas.SchemaQueryService
import com.openlattice.edm.type.AssociationType
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresQuery
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AssociationTypeMapstore
import com.openlattice.postgres.mapstores.EntityTypeMapstore
import com.openlattice.postgres.mapstores.PropertyTypeMapstore
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.sql.SQLException
import java.util.Queue
import java.util.UUID
import java.util.function.Consumer
import java.util.stream.Stream
import javax.annotation.Nonnull

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class PostgresTypeManager(
        val hds: HikariDataSource,
        hazelcastInstance: HazelcastInstance
): SchemaQueryService {
    private val schemas = HazelcastMap.SCHEMAS.getMap(hazelcastInstance)
    private val entityTypes = HazelcastMap.ENTITY_TYPES.getMap(hazelcastInstance)
    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(hazelcastInstance)
    private val associationTypes = HazelcastMap.ASSOCIATION_TYPES.getMap(hazelcastInstance)

    companion object {
        private val logger = LoggerFactory.getLogger(PostgresTypeManager::class.java)
        private val getEnumTypeIds = "SELECT ${PostgresColumn.ID.name} FROM ${PostgresTable.ENUM_TYPES.name}"
    }

    fun getPropertyTypesInNamespace(namespace: String): Iterable<PropertyType> {
        return propertyTypes.values(Predicates.equal(PropertyTypeMapstore.NAMESPACE_INDEX, namespace))
    }

    fun getPropertyTypes(): Iterable<PropertyType> {
        return propertyTypes.values
    }

    fun getEntityTypes(): Iterable<EntityType> {
        return entityTypes.values
    }

    fun getEntityTypesStrict(): Iterable<EntityType> {
        return entityTypes.values(Predicates.equal<UUID, EntityType>(EntityTypeMapstore.CATEGORY_INDEX, SecurableObjectType.EntityType))
    }

    fun getAssociationEntityTypes(): Iterable<EntityType> {
        return entityTypes.values(Predicates.equal<UUID, EntityType>(EntityTypeMapstore.CATEGORY_INDEX, SecurableObjectType.AssociationType))
    }

    fun getAssociationIdsForEntityType(entityTypeId: UUID): Set<UUID> {
        return associationTypes.keySet(Predicates.or(
                Predicates.equal<UUID, AssociationType>(AssociationTypeMapstore.SRC_INDEX, entityTypeId),
                Predicates.equal<UUID, AssociationType>(AssociationTypeMapstore.DST_INDEX, entityTypeId)
        ))
    }

    override fun getAllPropertyTypesInSchema(schemaName: FullQualifiedName): Set<UUID> {
        return propertyTypes.keySet( Predicates.equal<UUID, PropertyType>(PropertyTypeMapstore.SCHEMAS_INDEX, schemaName.fullQualifiedNameAsString))
    }

    override fun getAllEntityTypesInSchema(schemaName: FullQualifiedName): Set<UUID> {
        return entityTypes.keySet( Predicates.equal<UUID, EntityType>(EntityTypeMapstore.SCHEMAS_INDEX, schemaName.fullQualifiedNameAsString))
    }

    override fun getNamespaces(): Iterable<String> {
        return schemas.keys
    }
}