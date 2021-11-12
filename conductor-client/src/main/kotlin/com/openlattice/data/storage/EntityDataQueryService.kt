package com.openlattice.data.storage

import com.codahale.metrics.annotation.Timed
import com.openlattice.analysis.requests.Filter
import com.openlattice.data.*
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.streams.BasePostgresIterable
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.nio.ByteBuffer
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface EntityDataQueryService {
    fun getEntitySetCounts(): Map<UUID, Long>

    fun getEntitiesWithPropertyTypeIds(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>> = mapOf(),
            metadataOptions: Set<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            version: Optional<Long> = Optional.empty()
    ): Iterable<Pair<UUID, MutableMap<UUID, MutableSet<Any>>>>

    fun getLinkedEntitiesWithPropertyTypeIds(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>> = mapOf(),
            metadataOptions: Set<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            version: Optional<Long> = Optional.empty()
    ): Iterable<Pair<UUID, MutableMap<UUID, MutableSet<Any>>>>

    fun getEntitySetWithPropertyTypeIdsIterable(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java)
    ): Iterable<Pair<UUID, MutableMap<UUID, MutableSet<Any>>>>

    /**
     * Returns linked entity set data detailed in a Map mapped by linking id, (normal) entity set id, origin id,
     * property type id and values respectively.
     */
    fun getLinkedEntitiesByEntitySetIdWithOriginIds(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            propertyTypeFilters: Map<UUID, Set<Filter>> = mapOf(),
            version: Optional<Long> = Optional.empty()
    ): Iterable<Pair<UUID, Pair<UUID, Map<UUID, MutableMap<UUID, MutableSet<Any>>>>>>

    /**
     * Returns linked entity set data detailed in a Map mapped by linking id, (normal) entity set id, origin id,
     * property type full qualified name and values respectively.
     */
    fun getLinkedEntitySetBreakDown(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>> = mapOf(),
            version: Optional<Long> = Optional.empty()
    ): Iterable<Pair<UUID, Pair<UUID, Map<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>>>>>

    fun getEntitiesWithPropertyTypeFqns(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>> = mapOf(),
            metadataOptions: Set<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            version: Optional<Long> = Optional.empty(),
            linking: Boolean = false,
            filteredDataPageDefinition: FilteredDataPageDefinition? = null
    ): Map<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>>

    /**
     * Note: for linking queries, linking id and entity set id will be returned, thus data won't be merged by linking id
     */
    fun <T> getEntitySetIterable(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>> = mapOf(),
            metadataOptions: Set<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            version: Optional<Long> = Optional.empty(),
            linking: Boolean = false,
            detailed: Boolean = false,
            filteredDataPageDefinition: FilteredDataPageDefinition? = null,
            adapter: (ResultSet) -> T
    ): Iterable<T>

    /**
     * Updates or insert entities.
     * @param entitySetId The entity set id for which to insert entities for.
     * @param entities The entites to update or insert.
     * @param authorizedPropertyTypes The authorized property types for the insertion.
     * @param awsPassthrough True if the data will be stored directly in AWS via another means and all that is being
     * provided is the s3 prefix and key.
     *
     * @return A write event summarizing the results of performing this operation.
     */
    @Timed
    fun upsertEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            awsPassthrough: Boolean = false,
            propertyUpdateType: PropertyUpdateType,
    ): WriteEvent

    /**
     * Updates or insert entities.
     * @param entitySetId The entity set id for which to insert entities.
     * @param tombstoneFn A function that may tombstone values before performing the upsert.
     * @param entities The entities to update or insert.
     * @param authorizedPropertyTypes The authorized property types for the insertion.
     * @param version The version to use for upserting.
     * @param awsPassthrough True if the data will be stored directly in AWS via another means and all that is being
     * provided is the s3 prefix and key.
     *
     * @return A write event summarizing the results of performing this operation.
     */
    fun upsertEntities(
            entitySetId: UUID,
            tombstoneFn: (version: Long, entityBatch: Map<UUID, Map<UUID, Set<Any>>>) -> Unit,
            entities: Map<UUID, Map<UUID, Set<Any>>>, // ekids ->
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            version: Long,
            awsPassthrough: Boolean = false,
            propertyUpdateType: PropertyUpdateType
    ): WriteEvent

    fun upsertEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            version: Long,
            awsPassthrough: Boolean,
            propertyUpdateType: PropertyUpdateType
    ): Int

    fun getPropertyHash(
            entitySetId: UUID,
            entityKeyId: UUID,
            propertyTypeId: UUID,
            value: Any,
            dataType: EdmPrimitiveTypeKind,
            awsPassthrough: Boolean
    ): Pair<ByteArray, Any>

    @Timed
    fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            propertyUpdateType: PropertyUpdateType,
    ): WriteEvent

    @Timed
    fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            propertyUpdateType: PropertyUpdateType,
    ): WriteEvent

    @Timed
    fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>, // ekid -> ptid -> hashes -> shit
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            propertyUpdateType: PropertyUpdateType
    ): WriteEvent

    fun extractValues(propertyValues: Map<UUID, Set<Map<ByteBuffer, Any>>>): Map<UUID, Set<Any>>

    /**
     * Tombstones (writes a negative version) for the provided entity properties.
     * @param entitySetId The entity set to operate on.
     * @param entityKeyIds The entity key ids to tombstone.
     * @param authorizedPropertyTypes The property types the user is requested and is allowed to tombstone. We assume
     * that authorization checks are enforced at a higher level and that this just streamlines issuing the necessary
     * queries.
     */
    fun clearEntityData(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
    ): WriteEvent

    /**
     * Deletes properties of entities in entity set from [DATA] table.
     */
    fun deleteEntityData(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
    ): WriteEvent

    /**
     * Deletes properties of entities in entity set from [DATA] table.
     */
    fun deletePropertiesFromEntities(
            entitySetId: UUID,
            entities: Collection<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
    ): Int

    fun deletePropertyOfEntityFromS3(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            propertyTypeId: UUID
    )

    /**
     * Deletes entities from [IDS] table.
     */
    fun deleteEntities(entitySetId: UUID, entityKeyIds: Set<UUID>): WriteEvent

    /**
     * Tombstones the provided set of property types for each provided entity key.
     *
     * This version of tombstone only operates on the [DATA] table and does not change the version of
     * entities in the [IDS] table
     *
     * @param entitySetId The entity set id for which to tombstone entries
     * @param entityKeyIds The entity key ids for which to tombstone entries.
     * @param propertyTypesToTombstone A collection of property types to tombstone
     * @param version Version to be used for tombstoning.
     *
     * @return A write event object containing a summary of the operation useful for auditing purposes.
     */
    fun tombstone(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            propertyTypesToTombstone: Collection<PropertyType>,
            version: Long
    ): WriteEvent

    /**
     *
     * Tombstones the provided set of property type hash values for each provided entity key.
     *
     * This version of tombstone only operates on the [DATA] table and does not change the version of
     * entities in the [IDS] table
     *
     * @param entitySetId The entity set id for which to tombstone entries
     * @param entities The entities with their properties for which to tombstone entries.
     * @param version The version to use to tombstone.
     *
     * @return A write event object containing a summary of the operation useful for auditing purposes.
     *
     */
    fun tombstoneEntityPropertyHashes(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>,
            version: Long
    ): WriteEvent

    fun getExpiringEntitiesFromEntitySetUsingIds(
            entitySetId: UUID,
            expirationPolicy: DataExpiration,
            currentDateTime: OffsetDateTime
    ): BasePostgresIterable<UUID>

    fun getExpiringEntitiesFromEntitySetUsingData(
            entitySetId: UUID,
            expirationPolicy: DataExpiration,
            expirationPropertyType: PropertyType,
            currentDateTime: OffsetDateTime
    ): BasePostgresIterable<UUID>

    fun bindExpirationDate(
            ps: PreparedStatement,
            index: Int,
            expirationPolicy: DataExpiration,
            currentDateTime: OffsetDateTime,
            propertyType: PropertyType? = null
    )

    /**
     * PreparedStatement bind order:
     *
     * 1) entitySetId
     * 2) expiration date(time)
     * 3) propertyTypeId
     */
    fun getExpiringEntitiesUsingDataQuery(
            expirationPropertyType: PropertyType,
            deleteType: DeleteType
    ): String

    /**
     * PreparedStatement bind order:
     *
     * 1) entitySetId
     * 2) expiration datetime
     */
    fun getExpiringEntitiesUsingIdsQuery(expirationPolicy: DataExpiration): String
}