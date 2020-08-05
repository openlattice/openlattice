package com.openlattice.datastore.services;

import com.dataloom.mappers.ObjectMappers
import com.dataloom.streams.StreamUtil
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.SerializationFeature
import com.google.common.collect.*
import com.openlattice.IdConstants
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.conductor.rpc.ConductorElasticsearchApi
import com.openlattice.conductor.rpc.SearchConfiguration
import com.openlattice.data.EntityDataKey
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.Analyzer
import com.openlattice.edm.type.AssociationType
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.organizations.Organization
import com.openlattice.rhizome.hazelcast.DelegatedStringSet
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet
import com.openlattice.scrunchie.search.ElasticsearchTransportClientFactory
import com.openlattice.search.SortDefinition
import com.openlattice.search.SortType
import com.openlattice.search.requests.*
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.commons.lang3.tuple.Pair
import org.apache.lucene.search.join.ScoreMode
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.bulk.BulkItemResponse
import org.elasticsearch.action.search.MultiSearchRequest
import org.elasticsearch.action.search.MultiSearchResponse
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.geo.GeoPoint
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.DistanceUnit
import org.elasticsearch.common.unit.Fuzziness
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.*
import org.elasticsearch.index.reindex.DeleteByQueryAction
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.sort.*
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.io.IOException
import java.net.UnknownHostException
import java.util.*
import java.util.function.Consumer
import java.util.function.Function
import java.util.stream.Collectors
import java.util.stream.Stream

class DatastoreKotlinElasticsearchImpl(
        val config: SearchConfiguration,
        val someClient: Optional<Client>
) : ConductorElasticsearchApi {

    constructor(config: SearchConfiguration) : this(config, Optional.empty())

    companion object {
        private val MAX_CONCURRENT_SEARCHES = 3

        private val DEFAULT_INDICES = arrayOf(
                ConductorElasticsearchApi.ENTITY_SET_DATA_MODEL,
                ConductorElasticsearchApi.ORGANIZATIONS,
                ConductorElasticsearchApi.ENTITY_TYPE_INDEX,
                ConductorElasticsearchApi.ASSOCIATION_TYPE_INDEX,
                ConductorElasticsearchApi.PROPERTY_TYPE_INDEX,
                ConductorElasticsearchApi.APP_INDEX,
                ConductorElasticsearchApi.ENTITY_TYPE_COLLECTION_INDEX,
                ConductorElasticsearchApi.ENTITY_SET_COLLECTION_INDEX
        )

        private val indexNamesByObjectType = mapOf(
                SecurableObjectType.EntityType to ConductorElasticsearchApi.ENTITY_TYPE_INDEX,
                SecurableObjectType.AssociationType to ConductorElasticsearchApi.ASSOCIATION_TYPE_INDEX,
                SecurableObjectType.PropertyTypeInEntitySet to ConductorElasticsearchApi.PROPERTY_TYPE_INDEX,
                SecurableObjectType.App to ConductorElasticsearchApi.APP_INDEX,
                SecurableObjectType.EntityTypeCollection to ConductorElasticsearchApi.ENTITY_TYPE_COLLECTION_INDEX,
                SecurableObjectType.EntitySetCollection to ConductorElasticsearchApi.ENTITY_SET_COLLECTION_INDEX,
                SecurableObjectType.Organization to ConductorElasticsearchApi.ORGANIZATIONS
        )

        private val typeNamesByIndexName = mapOf(
                ConductorElasticsearchApi.ENTITY_TYPE_INDEX to ConductorElasticsearchApi.ENTITY_TYPE,
                ConductorElasticsearchApi.ASSOCIATION_TYPE_INDEX to ConductorElasticsearchApi.ASSOCIATION_TYPE,
                ConductorElasticsearchApi.PROPERTY_TYPE_INDEX to ConductorElasticsearchApi.PROPERTY_TYPE,
                ConductorElasticsearchApi.APP_INDEX to ConductorElasticsearchApi.APP,
                ConductorElasticsearchApi.ENTITY_TYPE_COLLECTION_INDEX to ConductorElasticsearchApi.ENTITY_TYPE_COLLECTION,
                ConductorElasticsearchApi.ENTITY_SET_COLLECTION_INDEX to ConductorElasticsearchApi.ENTITY_SET_COLLECTION,
                ConductorElasticsearchApi.ORGANIZATIONS to ConductorElasticsearchApi.ORGANIZATION_TYPE
        )

        private val mapper = ObjectMappers.newJsonMapper()
        private val logger = LoggerFactory.getLogger(DatastoreKotlinElasticsearchImpl::class.java)

        init {
            mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        }
    }

    private var connected = true
    private val server = config.elasticsearchUrl
    private val cluster = config.elasticsearchCluster
    private val port = config.elasticsearchPort
    private var factory = ElasticsearchTransportClientFactory(server, port, cluster)
    private var client: Client = someClient.orElseGet { factory.client }

    init {
        initializeIndices()
    }

    /* INDEX CREATION */

    /* INDEX CREATION */
    fun initializeIndices() {
        for (indexName in DEFAULT_INDICES) {
            createIndex(indexName)
        }
    }

    private fun createIndex(indexName: String?): Boolean {
        return when (indexName) {
            ConductorElasticsearchApi.ENTITY_SET_DATA_MODEL -> initializeEntitySetDataModelIndex()
            ConductorElasticsearchApi.ORGANIZATIONS -> initializeOrganizationIndex()
            else -> {
                initializeDefaultIndex(indexName, typeNamesByIndexName[indexName])
            }
        }
    }

    override fun getEntityTypesWithIndices(): Set<UUID>? {
        return Stream.of(*client!!.admin().indices().prepareGetIndex().setFeatures().get().indices)
                .filter { s: String -> s.startsWith(ConductorElasticsearchApi.DATA_INDEX_PREFIX) }
                .map { s: String -> UUID.fromString(s.substring(ConductorElasticsearchApi.DATA_INDEX_PREFIX.length)) }
                .collect(Collectors.toSet())
    }

    // @formatter:off
    @Throws(IOException::class) private  fun getMetaphoneSettings(  numShards:Int): XContentBuilder?{
        return XContentFactory.jsonBuilder()
                .startObject()
                .startObject(ConductorElasticsearchApi.ANALYSIS)
                .startObject(ConductorElasticsearchApi.FILTER)
                .startObject(ConductorElasticsearchApi.METAPHONE_FILTER)
                .field(ConductorElasticsearchApi.TYPE, ConductorElasticsearchApi.PHONETIC)
                .field(ConductorElasticsearchApi.ENCODER, ConductorElasticsearchApi.METAPHONE)
                .field(ConductorElasticsearchApi.REPLACE, false)
                .endObject()
                .startObject(ConductorElasticsearchApi.SHINGLE_FILTER)
                .field(ConductorElasticsearchApi.TYPE, ConductorElasticsearchApi.SHINGLE)
                .field(ConductorElasticsearchApi.OUTPUT_UNIGRAMS, true)
                .field(ConductorElasticsearchApi.TOKEN_SEPARATOR, "")
                .endObject()
                .endObject()
                .startObject(ConductorElasticsearchApi.ANALYZER)
                .startObject(ConductorElasticsearchApi.METAPHONE_ANALYZER)
                .field(ConductorElasticsearchApi.TOKENIZER, ConductorElasticsearchApi.LOWERCASE)
                .field(ConductorElasticsearchApi.FILTER, Lists.newArrayList(ConductorElasticsearchApi.LOWERCASE, ConductorElasticsearchApi.SHINGLE_FILTER, ConductorElasticsearchApi.METAPHONE_FILTER))
                .endObject()
                .endObject()
                .endObject()
                .field(ConductorElasticsearchApi.NUM_SHARDS, numShards)
                .field(ConductorElasticsearchApi.NUM_REPLICAS, 2)
                .endObject()
    }
    // @formatter:on

    // @formatter:on
    private fun indexExists(indexName: String?): Boolean {
        return client!!.admin().indices().prepareExists(indexName).execute().actionGet().isExists
    }

    private fun initializeEntitySetDataModelIndex(): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        if (indexExists(ConductorElasticsearchApi.ENTITY_SET_DATA_MODEL)) {
            return true
        }

        // entity_set type mapping
        val properties = ImmutableMap.builder<String, Any>()
        properties.put(
                ConductorElasticsearchApi.PROPERTY_TYPES,
                ImmutableMap.of(ConductorElasticsearchApi.TYPE, ConductorElasticsearchApi.NESTED)
        )
        properties.put(
                ConductorElasticsearchApi.ENTITY_SET,
                ImmutableMap.of(ConductorElasticsearchApi.TYPE, ConductorElasticsearchApi.OBJECT)
        )
        val typeTextAnalyzerMetaphoneAnalyzer: Map<String, String> = ImmutableMap
                .of(
                        ConductorElasticsearchApi.TYPE,
                        ConductorElasticsearchApi.TEXT,
                        ConductorElasticsearchApi.ANALYZER,
                        ConductorElasticsearchApi.METAPHONE_ANALYZER
                )
        properties.put(
                ConductorElasticsearchApi.ENTITY_SET + "." + SerializationConstants.NAME_FIELD,
                typeTextAnalyzerMetaphoneAnalyzer
        )
        properties.put(
                ConductorElasticsearchApi.ENTITY_SET + "." + SerializationConstants.TITLE_FIELD,
                typeTextAnalyzerMetaphoneAnalyzer
        )
        properties
                .put(
                        ConductorElasticsearchApi.ENTITY_SET + "." + SerializationConstants.DESCRIPTION_FIELD,
                        typeTextAnalyzerMetaphoneAnalyzer
                )
        val mapping: Map<String, Any> = ImmutableMap
                .of<String, Any>(
                        ConductorElasticsearchApi.ENTITY_SET_TYPE,
                        ImmutableMap.of(ConductorElasticsearchApi.MAPPING_PROPERTIES, properties.build())
                )
        return try {
            client!!.admin().indices().prepareCreate(ConductorElasticsearchApi.ENTITY_SET_DATA_MODEL)
                    .setSettings(getMetaphoneSettings(5))
                    .addMapping(ConductorElasticsearchApi.ENTITY_SET_TYPE, mapping)
                    .execute().actionGet()
            true
        } catch (e: IOException) {
            logger.error("Unable to initialize entity set data model index", e)
            false
        }
    }

    private fun initializeOrganizationIndex(): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        if (indexExists(ConductorElasticsearchApi.ORGANIZATIONS)) {
            return true
        }

        // entity_set type mapping
        val properties: Map<String, Any> = ImmutableMap.of<String, Any>(
                ConductorElasticsearchApi.ORGANIZATION,
                ImmutableMap.of(
                        ConductorElasticsearchApi.TYPE,
                        ConductorElasticsearchApi.OBJECT
                )
        )
        val organizationData: Map<String, Any> = ImmutableMap.of<String, Any>(
                ConductorElasticsearchApi.MAPPING_PROPERTIES,
                properties
        )
        client!!.admin().indices().prepareCreate(ConductorElasticsearchApi.ORGANIZATIONS)
                .setSettings(
                        Settings.builder()
                                .put(ConductorElasticsearchApi.NUM_SHARDS, 5)
                                .put(ConductorElasticsearchApi.NUM_REPLICAS, 2)
                )
                .addMapping(
                        ConductorElasticsearchApi.ORGANIZATION_TYPE,
                        ImmutableMap.of<String, Any>(ConductorElasticsearchApi.ORGANIZATION_TYPE, organizationData)
                )
                .execute().actionGet()
        return true
    }

    private fun initializeDefaultIndex(indexName: String?, typeName: String?): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        if (indexExists(indexName)) {
            return true
        }
        val mapping: Map<String?, Any> = ImmutableMap.of<String?, Any>(typeName, ImmutableMap.of<Any, Any>())
        client!!.admin().indices().prepareCreate(indexName)
                .setSettings(
                        Settings.builder()
                                .put(ConductorElasticsearchApi.NUM_SHARDS, 5)
                                .put(ConductorElasticsearchApi.NUM_REPLICAS, 2)
                )
                .addMapping(typeName, mapping)
                .execute().actionGet()
        return true
    }

    private fun getFieldMapping(propertyType: PropertyType): Map<String, String> {
        val fieldMapping: MutableMap<String, String> = Maps.newHashMap()
        when (propertyType.datatype) {
            EdmPrimitiveTypeKind.Boolean -> {
                fieldMapping[ConductorElasticsearchApi.TYPE] = ConductorElasticsearchApi.BOOLEAN
            }
            EdmPrimitiveTypeKind.SByte, EdmPrimitiveTypeKind.Byte -> {
                fieldMapping[ConductorElasticsearchApi.TYPE] = ConductorElasticsearchApi.BYTE
            }
            EdmPrimitiveTypeKind.Decimal -> {
                fieldMapping[ConductorElasticsearchApi.TYPE] = ConductorElasticsearchApi.FLOAT
            }
            EdmPrimitiveTypeKind.Double, EdmPrimitiveTypeKind.Single -> {
                fieldMapping[ConductorElasticsearchApi.TYPE] = ConductorElasticsearchApi.DOUBLE
            }
            EdmPrimitiveTypeKind.Int16 -> {
                fieldMapping[ConductorElasticsearchApi.TYPE] = ConductorElasticsearchApi.SHORT
            }
            EdmPrimitiveTypeKind.Int32 -> {
                fieldMapping[ConductorElasticsearchApi.TYPE] = ConductorElasticsearchApi.INTEGER
            }
            EdmPrimitiveTypeKind.Int64 -> {
                fieldMapping[ConductorElasticsearchApi.TYPE] = ConductorElasticsearchApi.LONG
            }
            EdmPrimitiveTypeKind.String -> {
                val analyzer = if (propertyType.analyzer == Analyzer.METAPHONE) ConductorElasticsearchApi.METAPHONE_ANALYZER else ConductorElasticsearchApi.STANDARD
                fieldMapping[ConductorElasticsearchApi.TYPE] = ConductorElasticsearchApi.TEXT
                fieldMapping[ConductorElasticsearchApi.ANALYZER] = analyzer
            }
            EdmPrimitiveTypeKind.Date, EdmPrimitiveTypeKind.DateTimeOffset -> {
                fieldMapping[ConductorElasticsearchApi.TYPE] = ConductorElasticsearchApi.DATE
            }
            EdmPrimitiveTypeKind.GeographyPoint -> {
                fieldMapping[ConductorElasticsearchApi.TYPE] = ConductorElasticsearchApi.GEO_POINT
            }
            EdmPrimitiveTypeKind.Guid -> {
                fieldMapping[ConductorElasticsearchApi.INDEX] = "false"
                fieldMapping[ConductorElasticsearchApi.TYPE] = ConductorElasticsearchApi.KEYWORD
            }
            else -> {
                fieldMapping[ConductorElasticsearchApi.INDEX] = "false"
                fieldMapping[ConductorElasticsearchApi.TYPE] = ConductorElasticsearchApi.KEYWORD
            }
        }
        if (propertyType.analyzer == Analyzer.NOT_ANALYZED) {
            fieldMapping[ConductorElasticsearchApi.ANALYZER] = ConductorElasticsearchApi.KEYWORD
        }
        return fieldMapping
    }

    private fun getIndexName(entityTypeId: UUID?): String {
        return ConductorElasticsearchApi.DATA_INDEX_PREFIX + entityTypeId
    }

    private fun getTypeName(entityTypeId: UUID): String {
        return ConductorElasticsearchApi.DATA_TYPE_PREFIX + entityTypeId
    }

    /*** ENTITY DATA INDEX CREATE / DELETE / UPDATE  */
    fun createEntityTypeDataIndex(entityType: EntityType, propertyTypes: List<PropertyType>): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        val entityTypeId = entityType.id
        val indexName = getIndexName(entityTypeId)
        val typeName = getTypeName(entityTypeId)
        val exists = client!!.admin().indices()
                .prepareExists(indexName).execute().actionGet().isExists
        if (exists) {
            return true
        }
        val entityTypeMapping = prepareEntityTypeDataMappings(typeName, propertyTypes)
        try {
            client!!.admin().indices().prepareCreate(indexName)
                    .setSettings(getMetaphoneSettings(entityType.shards))
                    .addMapping(typeName, entityTypeMapping)
                    .execute().actionGet()
        } catch (e: IOException) {
            logger.debug("unable to create entity type data index for {}", entityTypeId)
        }
        return true
    }

    private fun addMappingToEntityTypeDataIndex(
            entityType: EntityType,
            propertyTypes: List<PropertyType>
    ): Boolean {
        val indexName = getIndexName(entityType.id)
        val typeName = getTypeName(entityType.id)
        val entityTypeDataMapping = prepareEntityTypeDataMappings(typeName, propertyTypes)
        val request = PutMappingRequest(indexName)
        request.type(typeName)
        request.source(entityTypeDataMapping)
        try {
            client!!.admin().indices().putMapping(request).actionGet()
        } catch (e: IllegalStateException) {
            logger.debug("unable to add mapping to entity type data index for {}", entityType.id)
        }
        return true
    }

    private fun prepareEntityTypeDataMappings(
            typeName: String,
            propertyTypes: List<PropertyType>
    ): Map<String, Any?> {
        val keywordMapping: Map<String, Any> = ImmutableMap.of<String, Any>(
                ConductorElasticsearchApi.TYPE,
                ConductorElasticsearchApi.KEYWORD
        )
        // securable_object_row type mapping
        val entityPropertiesMapping = ImmutableMap.builder<String, Any>()
        entityPropertiesMapping.put(IdConstants.ENTITY_SET_ID_KEY_ID.id.toString(), keywordMapping)
        entityPropertiesMapping.put(
                IdConstants.LAST_WRITE_ID.id.toString(),
                ImmutableMap.of(ConductorElasticsearchApi.TYPE, ConductorElasticsearchApi.DATE)
        )
        for (propertyType in propertyTypes) {
            if (propertyType.datatype != EdmPrimitiveTypeKind.Binary) {
                entityPropertiesMapping.put(propertyType.id.toString(), getFieldMapping(propertyType))
            }
        }
        val entityMapping: Map<String, Any> = ImmutableMap.of<String, Any>(
                ConductorElasticsearchApi.MAPPING_PROPERTIES, entityPropertiesMapping.build(),
                ConductorElasticsearchApi.TYPE, ConductorElasticsearchApi.NESTED
        )
        val properties: Map<String, Any> = ImmutableMap.of<String, Any>(
                ConductorElasticsearchApi.ENTITY, entityMapping,
                ConductorElasticsearchApi.ENTITY_SET_ID_FIELD, keywordMapping
        )
        return ImmutableMap.of<String, Any?>(
                typeName, ImmutableMap.of(
                ConductorElasticsearchApi.MAPPING_PROPERTIES, properties
        )
        )
    }

    override fun saveEntitySetToElasticsearch(
            entityType: EntityType?,
            entitySet: EntitySet,
            propertyTypes: List<PropertyType?>
    ): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        val entitySetDataModel: Map<String, Any> = ImmutableMap.of(
                ConductorElasticsearchApi.ENTITY_SET, entitySet,
                ConductorElasticsearchApi.PROPERTY_TYPES, propertyTypes
        )
        try {
            val s = ObjectMappers.getJsonMapper().writeValueAsString(entitySetDataModel)
            client!!.prepareIndex(
                    ConductorElasticsearchApi.ENTITY_SET_DATA_MODEL,
                    ConductorElasticsearchApi.ENTITY_SET_TYPE,
                    entitySet.id.toString()
            )
                    .setSource(s, XContentType.JSON)
                    .execute().actionGet()
            return true
        } catch (e: JsonProcessingException) {
            logger.debug("error saving entity set to elasticsearch")
        }
        return false
    }

    /**
     * Add new mappings to existing index.
     * Updating the entity set model is handled in [.updatePropertyTypesInEntitySet]
     *
     * @param entityType       the entity type to which the new properties are added
     * @param newPropertyTypes the ids of the new properties
     */
    override fun addPropertyTypesToEntityType(entityType: EntityType, newPropertyTypes: List<PropertyType>): Boolean {
        saveObjectToElasticsearch(
                ConductorElasticsearchApi.ENTITY_TYPE_INDEX,
                ConductorElasticsearchApi.ENTITY_TYPE,
                entityType,
                entityType.id.toString()
        )
        return addMappingToEntityTypeDataIndex(entityType, newPropertyTypes)
    }

    override fun deleteEntitySet(entitySetId: UUID, entityTypeId: UUID?): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        client!!.prepareDelete(
                ConductorElasticsearchApi.ENTITY_SET_DATA_MODEL,
                ConductorElasticsearchApi.ENTITY_SET_TYPE,
                entitySetId.toString()
        ).execute().actionGet()
        val response = DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                .filter(QueryBuilders.termQuery(ConductorElasticsearchApi.ENTITY_SET_ID_FIELD, entitySetId.toString()))
                .source(getIndexName(entityTypeId))
                .get()
        logger.info(
                "Deleted {} documents from index {} for entity set {}",
                response.deleted,
                entityTypeId,
                entitySetId
        )
        return true
    }

    private fun getEntitySetIdFromHit(hit: SearchHit): UUID? {
        return UUID.fromString(hit.matchedQueries[0])
    }

    /*** ENTITY DATA CREATE/DELETE ***/

    /*** ENTITY DATA CREATE/DELETE  */
    /**
     * @param entityValues Property values of a linked entity mapped by the normal entity set id, normal entity key id
     * and property type ids respectively.
     */
    private fun formatLinkedEntity(entityValues: Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>): ByteArray? {
        val documents = entityValues.entries.stream().flatMap { esEntry: Map.Entry<UUID, Map<UUID, Map<UUID, Set<Any>>>> ->
            val entitySetId = esEntry.key
            val entity = esEntry.value // ek_id -> pt_id -> pt_values
            entity.entries.stream().map<Map<Any, Any>> { ekEntry: Map.Entry<UUID, Map<UUID, Set<Any>>> ->
                val entityKeyId = ekEntry.key
                val propertyValues = ekEntry.value
                val values: MutableMap<Any, Any> = HashMap(propertyValues.size + 2)
                propertyValues.forEach { (key: UUID, value: Set<Any>) -> values[key] = value }
                values[IdConstants.ID_ID.id] = entityKeyId
                values[IdConstants.ENTITY_SET_ID_KEY_ID.id] = entitySetId
                values
            }
        }.collect(Collectors.toList())
        return try {
            mapper.writeValueAsBytes(ImmutableMap.of(ConductorElasticsearchApi.ENTITY, documents))
        } catch (e: JsonProcessingException) {
            logger.debug("error creating linked entity data")
            null
        }
    }

    private fun formatEntity(entitySetId: UUID, entity: Map<UUID, Set<Any?>>): ByteArray? {
        val values: MutableMap<Any, Any> = HashMap(entity.size + 1)
        entity.forEach { (key: UUID, value: Set<Any?>) -> values[key] = value }
        values[IdConstants.ENTITY_SET_ID_KEY_ID.id] = entitySetId
        return try {
            mapper.writeValueAsBytes(
                    ImmutableMap.of(
                            ConductorElasticsearchApi.ENTITY,
                            values,
                            ConductorElasticsearchApi.ENTITY_SET_ID_FIELD,
                            entitySetId
                    )
            )
        } catch (e: JsonProcessingException) {
            logger.debug("error creating entity data")
            null
        }
    }

    override fun createEntityData(
            entityTypeId: UUID, edk: EntityDataKey, propertyValues: Map<UUID, Set<Any?>>
    ): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        val entitySetId = edk.entitySetId
        val entityKeyId = edk.entityKeyId
        val data = formatEntity(entitySetId, propertyValues)
        if (data != null) {
            client!!.prepareIndex(getIndexName(entityTypeId), getTypeName(entityTypeId), entityKeyId.toString())
                    .setSource(data, XContentType.JSON)
                    .execute().actionGet()
        }
        return data != null
    }

    override fun createBulkEntityData(
            entityTypeId: UUID,
            entitySetId: UUID,
            entitiesById: Map<UUID, Map<UUID, Set<Any?>>>
    ): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        if (!entitiesById.isEmpty()) {
            val indexName = getIndexName(entityTypeId)
            val indexType = getTypeName(entityTypeId)
            val requestBuilder = client!!.prepareBulk()
            entitiesById.forEach { (entityKeyId: UUID, entityData: Map<UUID, Set<Any?>>) ->
                val data = formatEntity(entitySetId, entityData)
                if (data != null) {
                    requestBuilder.add(
                            client!!.prepareIndex(indexName, indexType, entityKeyId.toString())
                                    .setSource(data, XContentType.JSON)
                    )
                }
            }
            val resp = requestBuilder.execute().actionGet()
            if (resp.hasFailures()) {
                logger.info(
                        "At least one failure observed when attempting to index {} entities for entity set {}: {}",
                        entitiesById.size,
                        entitySetId,
                        resp.buildFailureMessage()
                )
                return false
            }
        }
        return true
    }

    override fun createBulkLinkedData(
            entityTypeId: UUID,
            entitiesByLinkingId: Map<UUID, Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>>
    ): Boolean { // linking_id/entity_set_id/entity_key_id/property_id
        if (!verifyElasticsearchConnection()) {
            return false
        }
        if (!entitiesByLinkingId.isEmpty()) {
            val indexName = getIndexName(entityTypeId)
            val indexType = getTypeName(entityTypeId)
            val requestBuilder = client!!.prepareBulk()
            entitiesByLinkingId.forEach { (linkingId: UUID, entityValues: Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>) ->
                val data = formatLinkedEntity(entityValues)
                if (data != null) {
                    requestBuilder.add(
                            client!!.prepareIndex(indexName, indexType, linkingId.toString())
                                    .setSource(data, XContentType.JSON)
                    )
                }
            }
            val resp = requestBuilder.execute().actionGet()
            if (resp.hasFailures()) {
                logger.info(
                        "At least one failure observed when attempting to index linking entities with linking " +
                                "ids {}: {}",
                        entitiesByLinkingId.keys,
                        resp.buildFailureMessage()
                )
                return false
            }
        }
        return true
    }

    override fun deleteEntityDataBulk(entityTypeId: UUID, entityKeyIds: Set<UUID>): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        val index = getIndexName(entityTypeId)
        val type = getTypeName(entityTypeId)
        val request = client!!.prepareBulk()
        entityKeyIds.forEach(Consumer { entityKeyId: UUID ->
            request.add(
                    client!!.prepareDelete(
                            index,
                            type,
                            entityKeyId.toString()
                    )
            )
        }
        )
        request.execute().actionGet()
        return true
    }

    override fun clearEntitySetData(entitySetId: UUID, entityTypeId: UUID?): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        val resp = DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                .filter(QueryBuilders.termQuery(ConductorElasticsearchApi.ENTITY_SET_ID_FIELD, entitySetId.toString()))
                .source(getIndexName(entityTypeId))
                .execute()
                .actionGet()
        logger.info(
                "Deleted {} normal entity documents while deleting entity set data {}", resp.deleted,
                entitySetId
        )
        return true
    }

    /*** ENTITY DATA SEARCH HELPERS  */
    private fun getEntityDataKeySearchResult(response: MultiSearchResponse): EntityDataKeySearchResult? {
        val entityDataKeys: MutableList<EntityDataKey> = Lists.newArrayList()
        var totalHits = 0
        for (item in response.responses) {
            for (hit in item.response.hits) {
                entityDataKeys.add(
                        EntityDataKey(
                                getEntitySetIdFromHit(hit),
                                UUID.fromString(hit.id)
                        )
                )
            }
            totalHits += item.response.hits.totalHits.value.toInt()
        }
        return EntityDataKeySearchResult(totalHits.toLong(), entityDataKeys)
    }

    /**
     * Creates for each authorized property type a map with key of that property type id and puts 1 weights as value.
     */
    private fun getFieldsMap(
            entitySetId: UUID,
            authorizedPropertyTypesByEntitySet: Map<UUID, DelegatedUUIDSet>
    ): Map<UUID, Map<String, Float>> {
        val fieldsMap: MutableMap<UUID, Map<String, Float>> = Maps.newHashMap()
        authorizedPropertyTypesByEntitySet[entitySetId]!!.forEach(Consumer { propertyTypeId: UUID ->
            val fieldName = getFieldName(propertyTypeId)

            // authorized property types are the same within 1 linking entity set (no need for extra check)
            fieldsMap[propertyTypeId] = java.util.Map.of(fieldName, 1f)
        })
        return fieldsMap
    }

    private fun getFieldName(propertyTypeId: UUID?): String {
        return ConductorElasticsearchApi.ENTITY + "." + propertyTypeId
    }

    private fun getAdvancedSearchQuery(
            constraints: Constraint,
            authorizedFieldsMap: Map<UUID, Map<String, Float>>
    ): BoolQueryBuilder {
        val query = QueryBuilders.boolQuery().minimumShouldMatch(1)
        for (search in constraints.searches.get()) {
            if (authorizedFieldsMap.keys.contains(search.propertyType)) {
                val queryString = QueryBuilders
                        .queryStringQuery(search.searchTerm)
                        .fields(authorizedFieldsMap[search.propertyType]).lenient(true)
                if (search.exactMatch) {
                    query.must(queryString)
                    query.minimumShouldMatch(0)
                } else {
                    query.should(queryString)
                }
            }
        }
        return query
    }

    private fun getSimpleSearchQuery(
            constraints: Constraint,
            authorizedFieldsMap: Map<UUID, Map<String, Float>>
    ): QueryBuilder? {
        val searchTerm = constraints.searchTerm.get()
        val formattedSearchTerm = if (constraints.fuzzy.get()) getFormattedFuzzyString(searchTerm) else searchTerm

        return QueryBuilders.queryStringQuery(formattedSearchTerm)
                .fields(
                        authorizedFieldsMap.values.flatMap {
                            it.entries
                        }.map {
                            it.key to it.value
                        }.toMap()
                ).lenient(true)
    }

    private fun getGeoDistanceSearchQuery(
            constraints: Constraint,
            authorizedFieldsMap: Map<UUID, Map<String, Float>>
    ): BoolQueryBuilder? {
        val propertyTypeId = constraints.propertyTypeId.get()
        if ((authorizedFieldsMap[propertyTypeId] ?: ImmutableMap.of()).size == 0) {
            return null
        }
        val latitude = constraints.latitude.get()
        val longitude = constraints.longitude.get()
        val radius = constraints.radius.get()
        val query = QueryBuilders.boolQuery().minimumShouldMatch(1)
        authorizedFieldsMap[propertyTypeId]!!.keys.forEach(Consumer { fieldName: String? ->
            query.should(
                    QueryBuilders
                            .geoDistanceQuery(fieldName)
                            .point(latitude, longitude)
                            .distance(radius, DistanceUnit.fromString(constraints.distanceUnit.get().name))
            )
        }
        )
        return query
    }

    private fun getGeoPolygonSearchQuery(
            constraints: Constraint,
            authorizedFieldsMap: Map<UUID, Map<String, Float>>
    ): BoolQueryBuilder? {
        val propertyTypeId = constraints.propertyTypeId.get()
        if ((authorizedFieldsMap[propertyTypeId] ?: ImmutableMap.of()).size == 0) {
            return null
        }
        val query = QueryBuilders.boolQuery().minimumShouldMatch(1)
        for (zone in constraints.zones.get()) {
            val polygon = zone.stream().map { pair: List<Double> -> GeoPoint(pair[1], pair[0]) }
                    .collect(Collectors.toList())
            authorizedFieldsMap[propertyTypeId]!!.keys
                    .forEach(Consumer { fieldName: String? ->
                        query.should(
                                QueryBuilders.geoPolygonQuery(
                                        fieldName,
                                        polygon
                                )
                        )
                    })
        }
        return query
    }

    private fun getWriteDateTimeFilterQuery(entitySetIds: Array<UUID>, constraint: Constraint): QueryBuilder? {
        val query = QueryBuilders.boolQuery().minimumShouldMatch(1)
        for (i in entitySetIds.indices) {
            val rangeQuery = QueryBuilders.rangeQuery(getFieldName(IdConstants.LAST_WRITE_ID.id))
            if (constraint.startDate.isPresent) {
                rangeQuery.gt(constraint.startDate.get().toString())
            }
            if (constraint.endDate.isPresent) {
                rangeQuery.lte(constraint.endDate.get().toString())
            }
            query.should(rangeQuery)
        }
        return query
    }

    private fun getQueryForSearch(
            entitySetIds: Set<UUID>,
            searchConstraints: SearchConstraints,
            authorizedFieldsMap: Map<UUID, Map<String, Float>>
    ): QueryBuilder? {
        val query = QueryBuilders.boolQuery()
        if (authorizedFieldsMap.size == 0) {
            return null
        }
        for (constraintGroup in searchConstraints.constraintGroups) {
            val subQuery = QueryBuilders.boolQuery()
                    .minimumShouldMatch(constraintGroup.minimumMatches)
            for (constraint in constraintGroup.constraints) {
                when (constraint.searchType) {
                    SearchType.advanced -> {
                        val advancedSearchQuery = getAdvancedSearchQuery(
                                constraint,
                                authorizedFieldsMap
                        )
                        if (advancedSearchQuery.hasClauses()) subQuery.should(advancedSearchQuery)
                    }
                    SearchType.geoDistance -> {
                        val geoDistanceSearchQuery = getGeoDistanceSearchQuery(
                                constraint,
                                authorizedFieldsMap
                        )
                        if (geoDistanceSearchQuery!!.hasClauses()) subQuery.should(geoDistanceSearchQuery)
                    }
                    SearchType.geoPolygon -> {
                        val geoPolygonSearchQuery = getGeoPolygonSearchQuery(
                                constraint,
                                authorizedFieldsMap
                        )
                        if (geoPolygonSearchQuery!!.hasClauses()) subQuery.should(geoPolygonSearchQuery)
                    }
                    SearchType.simple -> subQuery.should(getSimpleSearchQuery(constraint, authorizedFieldsMap))
                    SearchType.writeDateTimeFilter -> subQuery.should(
                            getWriteDateTimeFilterQuery(
                                    searchConstraints.entitySetIds,
                                    constraint
                            )
                    )
                }
            }
            if (!subQuery.hasClauses()) {
                return null
            }
            query.must(QueryBuilders.nestedQuery(ConductorElasticsearchApi.ENTITY, subQuery, ScoreMode.Total))
        }
        val entitySetQuery = QueryBuilders.boolQuery().minimumShouldMatch(1)
        entitySetIds.forEach(Consumer { entitySetId: UUID ->
            entitySetQuery.should(
                    QueryBuilders.termQuery(
                            getFieldName(IdConstants.ENTITY_SET_ID_KEY_ID.id),
                            entitySetId.toString()
                    )
            )
        })
        query.must(QueryBuilders.nestedQuery(ConductorElasticsearchApi.ENTITY, entitySetQuery, ScoreMode.Max))
        return query
    }

    /*** ENTITY DATA SEARCH  */
    override fun executeSearch(
            searchConstraints: SearchConstraints,
            entityTypesByEntitySetId: Map<UUID?, UUID?>,
            authorizedPropertyTypesByEntitySet: Map<UUID, DelegatedUUIDSet>,
            linkingEntitySets: Map<UUID?, DelegatedUUIDSet>
    ): EntityDataKeySearchResult? {
        if (!verifyElasticsearchConnection()) {
            return EntityDataKeySearchResult(0, ImmutableList.of())
        }
        val sort = buildSort(searchConstraints.sortDefinition)
        val requests = MultiSearchRequest().maxConcurrentSearchRequests(MAX_CONCURRENT_SEARCHES)
        for (i in searchConstraints.entitySetIds.indices) {
            val entitySetId = searchConstraints.entitySetIds[i]
            val normalEntitySets: Set<UUID> = linkingEntitySets[entitySetId]
                    ?: DelegatedUUIDSet.wrap(ImmutableSet.of(entitySetId))
            val authorizedFieldsMap = getFieldsMap(entitySetId, authorizedPropertyTypesByEntitySet)
            val searchQuery = getQueryForSearch(normalEntitySets, searchConstraints, authorizedFieldsMap)
            if (searchQuery != null) {
                val query = BoolQueryBuilder().queryName(entitySetId.toString()).must(searchQuery)
                if (linkingEntitySets.containsKey(entitySetId)) {
                    query.mustNot(
                            QueryBuilders
                                    .existsQuery(ConductorElasticsearchApi.ENTITY_SET_ID_FIELD)
                    ) // this field will not exist for linked entity
                    // documents
                } else {
                    query.must(
                            QueryBuilders
                                    .termQuery(ConductorElasticsearchApi.ENTITY_SET_ID_FIELD, entitySetId.toString())
                    ) // match entity set id
                }
                val request = client!!
                        .prepareSearch(getIndexName(entityTypesByEntitySetId[entitySetId]))
                        .setQuery(query)
                        .setTrackTotalHits(true)
                        .setFrom(searchConstraints.start)
                        .setSize(searchConstraints.maxHits)
                        .addSort(sort)
                        .setFetchSource(false)
                requests.add(request)
            }
        }
        if (requests.requests().isEmpty()) {
            return EntityDataKeySearchResult(0, ImmutableList.of())
        }
        val response = client!!.multiSearch(requests).actionGet()
        return getEntityDataKeySearchResult(response)
    }

    override fun executeBlockingSearch(
            entityTypeId: UUID?,
            fieldSearches: Map<UUID?, DelegatedStringSet>,
            size: Int,
            explain: Boolean
    ): Map<UUID, Set<UUID>> {
        require(verifyElasticsearchConnection()) {
            "A connection to the search service is required."
        }
        val valuesQuery = BoolQueryBuilder()
        fieldSearches.entries.stream().forEach { entry: Map.Entry<UUID?, DelegatedStringSet> ->
            val fieldQuery = BoolQueryBuilder()
            entry.value.stream().forEach { searchTerm: String ->
                fieldQuery.should(
                        mustMatchQuery(getFieldName(entry.key), searchTerm).fuzziness(Fuzziness.AUTO)
                                .lenient(true)
                )
            }
            fieldQuery.minimumShouldMatch(1)
            valuesQuery.should(QueryBuilders.nestedQuery(ConductorElasticsearchApi.ENTITY, fieldQuery, ScoreMode.Avg))
        }
        valuesQuery.minimumShouldMatch(1)
        val query = QueryBuilders.boolQuery().must(valuesQuery)
                .must(QueryBuilders.existsQuery(ConductorElasticsearchApi.ENTITY_SET_ID_FIELD))
        return client!!.prepareSearch(getIndexName(entityTypeId))
                .setQuery(query)
                .setFrom(0)
                .setSize(size)
                .setExplain(explain)
                .setFetchSource(ConductorElasticsearchApi.ENTITY_SET_ID_FIELD, null)
                .execute()
                .actionGet().hits.asSequence()
                .map { hit: SearchHit ->
                    Pair
                            .of(
                                    UUID.fromString(hit.sourceAsMap[ConductorElasticsearchApi.ENTITY_SET_ID_FIELD].toString()),
                                    UUID.fromString(hit.id)
                            )
                }
                .groupingBy { it.key }
                .fold({ _, _ -> mutableSetOf<UUID>() }) { _, acc, elem ->
                    acc.add(elem.value)
                    acc
                }
    }

    /*** EDM OBJECT CRUD TRIGGERING INDEX UPDATES  */
    override fun updateOrganization(
            id: UUID, optionalTitle: Optional<String?>, optionalDescription: Optional<String?>
    ): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        val updatedFields: MutableMap<String, Any> = Maps.newHashMap()
        if (optionalTitle.isPresent) {
            updatedFields[SerializationConstants.TITLE_FIELD] = optionalTitle.get()
        }
        if (optionalDescription.isPresent) {
            updatedFields[SerializationConstants.DESCRIPTION_FIELD] = optionalDescription.get()
        }
        try {
            val s = ObjectMappers.getJsonMapper().writeValueAsString(updatedFields)
            val updateRequest = UpdateRequest(
                    ConductorElasticsearchApi.ORGANIZATIONS,
                    ConductorElasticsearchApi.ORGANIZATION_TYPE,
                    id.toString()
            )
                    .doc(s, XContentType.JSON)
            client!!.update(updateRequest).actionGet()
            return true
        } catch (e: IOException) {
            logger.debug("error updating organization in elasticsearch")
        }
        return false
    }

    override fun saveEntityTypeToElasticsearch(entityType: EntityType, propertyTypes: List<PropertyType>): Boolean {
        saveObjectToElasticsearch(
                ConductorElasticsearchApi.ENTITY_TYPE_INDEX,
                ConductorElasticsearchApi.ENTITY_TYPE,
                entityType,
                entityType.id.toString()
        )
        return createEntityTypeDataIndex(entityType, propertyTypes)
    }

    override fun saveAssociationTypeToElasticsearch(
            associationType: AssociationType,
            propertyTypes: List<PropertyType>
    ): Boolean {
        val entityType = associationType.associationEntityType
        if (entityType == null) {
            logger.debug("An association type must have an entity type present in order to save to elasticsearch")
            return false
        }
        saveObjectToElasticsearch(
                ConductorElasticsearchApi.ASSOCIATION_TYPE_INDEX,
                ConductorElasticsearchApi.ASSOCIATION_TYPE,
                associationType,
                entityType.id.toString()
        )
        return createEntityTypeDataIndex(entityType, propertyTypes)
    }

    override fun saveSecurableObjectToElasticsearch(
            securableObjectType: SecurableObjectType, securableObject: Any
    ): Boolean {
        val indexName = indexNamesByObjectType[securableObjectType]
        val typeName = typeNamesByIndexName[indexName]
        val id = getIdFnForType(securableObjectType).apply(securableObject)
        return saveObjectToElasticsearch(indexName, typeName, securableObject, id)
    }

    override fun deleteSecurableObjectFromElasticsearch(
            securableObjectType: SecurableObjectType, objectId: UUID
    ): Boolean {
        if (securableObjectType == SecurableObjectType.EntityType || (securableObjectType
                        == SecurableObjectType.AssociationType)) {
            client!!.admin().indices()
                    .delete(DeleteIndexRequest(getIndexName(objectId)))
        }
        val indexName = indexNamesByObjectType[securableObjectType]
        val typeName = typeNamesByIndexName[indexName]
        return deleteObjectById(indexName, typeName, objectId.toString())
    }

    override fun updateEntitySetMetadata(entitySet: EntitySet): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        val entitySetObj: MutableMap<String, Any> = Maps.newHashMap()
        entitySetObj[ConductorElasticsearchApi.ENTITY_SET] = entitySet
        try {
            val s = ObjectMappers.getJsonMapper().writeValueAsString(entitySetObj)
            val updateRequest = UpdateRequest(
                    ConductorElasticsearchApi.ENTITY_SET_DATA_MODEL,
                    ConductorElasticsearchApi.ENTITY_SET_TYPE,
                    entitySet.id.toString()
            ).doc(s, XContentType.JSON)
            client!!.update(updateRequest).actionGet()
            return true
        } catch (e: IOException) {
            logger.debug("error updating entity set metadata in elasticsearch")
        }
        return false
    }

    override fun updatePropertyTypesInEntitySet(entitySetId: UUID, updatedPropertyTypes: List<PropertyType?>): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        val propertyTypes: MutableMap<String, Any> = Maps.newHashMap()
        propertyTypes[ConductorElasticsearchApi.PROPERTY_TYPES] = updatedPropertyTypes
        try {
            val s = ObjectMappers.getJsonMapper().writeValueAsString(propertyTypes)
            val updateRequest = UpdateRequest(
                    ConductorElasticsearchApi.ENTITY_SET_DATA_MODEL,
                    ConductorElasticsearchApi.ENTITY_SET_TYPE,
                    entitySetId.toString()
            ).doc(s, XContentType.JSON)
            client!!.update(updateRequest).actionGet()
            return true
        } catch (e: IOException) {
            logger.debug("error updating property types of entity set in elasticsearch")
        }
        return false
    }

    override fun createOrganization(organization: Organization): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        try {
            val s = ObjectMappers.getJsonMapper().writeValueAsString(getOrganizationObject(organization))
            client!!.prepareIndex(
                    ConductorElasticsearchApi.ORGANIZATIONS,
                    ConductorElasticsearchApi.ORGANIZATION_TYPE,
                    organization.id.toString()
            )
                    .setSource(s, XContentType.JSON)
                    .execute().actionGet()
            return true
        } catch (e: JsonProcessingException) {
            logger.debug("error creating organization in elasticsearch")
        }
        return false
    }

    /*** METADATA SEARCHES  */
    override fun executeSecurableObjectSearch(
            securableObjectType: SecurableObjectType, searchTerm: String, start: Int, maxHits: Int
    ): SearchResult? {
        if (!verifyElasticsearchConnection()) {
            return SearchResult(0, Lists.newArrayList())
        }
        val fieldsMap = getFieldsMap(securableObjectType)
        val indexName = indexNamesByObjectType[securableObjectType]
        val typeName = typeNamesByIndexName[indexName]
        val query: QueryBuilder = QueryBuilders.queryStringQuery(getFormattedFuzzyString(searchTerm)).fields(fieldsMap)
                .lenient(true)
        val response = client!!.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(query)
                .setFrom(start)
                .setSize(maxHits)
                .execute()
                .actionGet()
        val hits: MutableList<Map<String, Any>> = Lists.newArrayList()
        for (hit in response.hits) {
            hits.add(hit.sourceAsMap)
        }
        return SearchResult(response.hits.totalHits.value, hits)
    }

    override fun executeSecurableObjectFQNSearch(
            securableObjectType: SecurableObjectType, namespace: String, name: String, start: Int, maxHits: Int
    ): SearchResult? {
        if (!verifyElasticsearchConnection()) {
            return SearchResult(0, Lists.newArrayList())
        }
        val indexName = indexNamesByObjectType[securableObjectType]
        val typeName = typeNamesByIndexName[indexName]
        val query = BoolQueryBuilder()
        query.must(
                QueryBuilders
                        .regexpQuery(
                                SerializationConstants.TYPE_FIELD + "." + SerializationConstants.NAMESPACE_FIELD,
                                ".*$namespace.*"
                        )
        )
                .must(
                        QueryBuilders
                                .regexpQuery(
                                        SerializationConstants.TYPE_FIELD + "." + SerializationConstants.NAME_FIELD,
                                        ".*$name.*"
                                )
                )
        val response = client!!.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(query)
                .setFrom(start)
                .setSize(maxHits)
                .execute()
                .actionGet()
        val hits: MutableList<Map<String, Any>> = Lists.newArrayList()
        for (hit in response.hits) {
            hits.add(hit.sourceAsMap)
        }
        return SearchResult(response.hits.totalHits.value, hits)
    }

    override fun executeEntitySetCollectionSearch(
            searchTerm: String?, authorizedEntitySetCollectionIds: Set<AclKey?>?, start: Int, maxHits: Int
    ): SearchResult? {
        return null
    }

    override fun executeOrganizationSearch(
            searchTerm: String?,
            authorizedOrganizationIds: Set<AclKey>,
            start: Int,
            maxHits: Int
    ): SearchResult? {
        if (!verifyElasticsearchConnection()) {
            return SearchResult(0, Lists.newArrayList())
        }

        val query = BoolQueryBuilder()
                .should(
                        QueryBuilders.queryStringQuery(searchTerm).field(SerializationConstants.TITLE_FIELD)
                                .lenient(true).fuzziness(Fuzziness.AUTO)
                )
                .should(
                        QueryBuilders.queryStringQuery(searchTerm).field(SerializationConstants.DESCRIPTION_FIELD)
                                .lenient(true).fuzziness(Fuzziness.AUTO)
                )
                .minimumShouldMatch(1)

        query.filter(
                QueryBuilders.idsQuery()
                        .addIds(*authorizedOrganizationIds.map {
                            it[0].toString()
                        }.toTypedArray())
        )

        val response = client!!.prepareSearch(ConductorElasticsearchApi.ORGANIZATIONS)
                .setTypes(ConductorElasticsearchApi.ORGANIZATION_TYPE)
                .setQuery(query)
                .setFrom(start)
                .setSize(maxHits)
                .execute()
                .actionGet()
        val hits: MutableList<Map<String, Any>> = Lists.newArrayList()
        for (hit in response.hits) {
            val hitMap = hit.sourceAsMap
            hitMap["id"] = hit.id
            hits.add(hitMap)
        }
        return SearchResult(response.hits.totalHits.value, hits)
    }

    override fun executeEntitySetMetadataSearch(
            optionalSearchTerm: Optional<String?>,
            optionalEntityType: Optional<UUID?>,
            optionalPropertyTypes: Optional<Set<UUID>?>,
            authorizedAclKeys: Set<AclKey>,
            start: Int,
            maxHits: Int
    ): SearchResult? {
        if (!verifyElasticsearchConnection()) {
            return SearchResult(0, Lists.newArrayList())
        }
        val query = BoolQueryBuilder()
        if (optionalSearchTerm.isPresent) {
            val searchTerm = optionalSearchTerm.get()
            val fieldsMap: MutableMap<String, Float> = Maps.newHashMap()
            fieldsMap[ConductorElasticsearchApi.ENTITY_SET + "." + SerializationConstants.ID_FIELD] = 1f
            fieldsMap[ConductorElasticsearchApi.ENTITY_SET + "." + SerializationConstants.NAME] = 1f
            fieldsMap[ConductorElasticsearchApi.ENTITY_SET + "." + SerializationConstants.TITLE_FIELD] = 1f
            fieldsMap[ConductorElasticsearchApi.ENTITY_SET + "." + SerializationConstants.DESCRIPTION_FIELD] = 1f
            query.must(
                    QueryBuilders.queryStringQuery(getFormattedFuzzyString(searchTerm)).fields(fieldsMap)
                            .lenient(true).fuzziness(Fuzziness.AUTO)
            )
        }
        if (optionalEntityType.isPresent) {
            val eid = optionalEntityType.get()
            query.must(
                    mustMatchQuery(
                            ConductorElasticsearchApi.ENTITY_SET + "." + SerializationConstants.ENTITY_TYPE_ID,
                            eid.toString()
                    )
            )
        } else if (optionalPropertyTypes.isPresent) {
            val propertyTypes = optionalPropertyTypes.get()
            for (pid in propertyTypes) {
                query.must(
                        QueryBuilders.nestedQuery(
                                ConductorElasticsearchApi.PROPERTY_TYPES,
                                mustMatchQuery(
                                        ConductorElasticsearchApi.PROPERTY_TYPES + "." + SerializationConstants.ID_FIELD,
                                        pid.toString()
                                ),
                                ScoreMode.Avg
                        )
                )
            }
        }

        query.filter(
                QueryBuilders.idsQuery()
                        .addIds(*authorizedAclKeys.map {
                            it[0].toString()
                        }.toTypedArray())
        )

        val response = client!!.prepareSearch(ConductorElasticsearchApi.ENTITY_SET_DATA_MODEL)
                .setTypes(ConductorElasticsearchApi.ENTITY_SET_TYPE)
                .setQuery(query)
                .setFetchSource(
                        arrayOf(ConductorElasticsearchApi.ENTITY_SET, ConductorElasticsearchApi.PROPERTY_TYPES),
                        null
                )
                .setFrom(start)
                .setSize(maxHits)
                .execute()
                .actionGet()
        val hits: MutableList<Map<String, Any>> = Lists.newArrayList()
        response.hits.forEach(Consumer { hit: SearchHit -> hits.add(hit.sourceAsMap) })
        return SearchResult(response.hits.totalHits.value, hits)
    }

    /*** RE-INDEXING  */
    private fun getIdFnForType(securableObjectType: SecurableObjectType): Function<Any, String> {
        return when (securableObjectType) {
            SecurableObjectType.AssociationType -> Function { at: Any -> (at as AssociationType).associationEntityType.id.toString() }
            SecurableObjectType.Organization -> Function { o: Any -> (o as Organization).id.toString() }
            else -> Function { aso: Any -> (aso as AbstractSecurableObject).id.toString() }
        }
    }

    override fun triggerEntitySetIndex(
            entitySets: Map<EntitySet?, Set<UUID?>>,
            propertyTypes: Map<UUID?, PropertyType?>
    ): Boolean {
        val idFn = Function { map: Any -> (map as Map<String?, EntitySet>)[ConductorElasticsearchApi.ENTITY_SET]!!.id.toString() }
        val entitySetMaps = entitySets.entries.stream().map<Map<String, Any?>> { entry: Map.Entry<EntitySet?, Set<UUID?>> ->
            val entitySetMap: MutableMap<String, Any?> = Maps.newHashMap()
            entitySetMap[ConductorElasticsearchApi.ENTITY_SET] = entry.key
            entitySetMap[ConductorElasticsearchApi.PROPERTY_TYPES] = entry.value.stream().map { key: UUID? -> propertyTypes[key] }.collect(
                    Collectors.toList()
            )
            entitySetMap
        }.collect(Collectors.toList())
        return triggerIndex(
                ConductorElasticsearchApi.ENTITY_SET_DATA_MODEL,
                ConductorElasticsearchApi.ENTITY_SET_TYPE,
                entitySetMaps,
                idFn
        )
    }

    override fun triggerOrganizationIndex(organizations: List<Organization>): Boolean {
        val idFn = Function { org: Any ->
            (org as Map<String?, Any>)[SerializationConstants.ID_FIELD].toString()
        }

        val organizationObjects = organizations.map {
            getOrganizationObject(it)
        }.toList()

        return triggerIndex(
                ConductorElasticsearchApi.ORGANIZATIONS,
                ConductorElasticsearchApi.ORGANIZATION_TYPE,
                organizationObjects,
                idFn
        )
    }

    override fun triggerSecurableObjectIndex(
            securableObjectType: SecurableObjectType,
            securableObjects: Iterable<*>
    ): Boolean {
        val indexName = indexNamesByObjectType[securableObjectType]
        val typeName = typeNamesByIndexName[indexName]
        return triggerIndex(indexName, typeName, securableObjects, getIdFnForType(securableObjectType))
    }

    //TODO: Seems dangerous and like we should delete?
    fun clearAllData(): Boolean {
        client!!.admin().indices()
                .delete(DeleteIndexRequest(ConductorElasticsearchApi.DATA_INDEX_PREFIX + "*"))
        DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                .filter(QueryBuilders.matchAllQuery()).source(
                        ConductorElasticsearchApi.ENTITY_SET_DATA_MODEL,
                        ConductorElasticsearchApi.ENTITY_TYPE_INDEX,
                        ConductorElasticsearchApi.PROPERTY_TYPE_INDEX,
                        ConductorElasticsearchApi.ASSOCIATION_TYPE_INDEX,
                        ConductorElasticsearchApi.ORGANIZATIONS,
                        ConductorElasticsearchApi.APP_INDEX
                )
                .get()
        return true
    }


    /* HELPERS */

    /* HELPERS */
    private fun getFormattedFuzzyString(searchTerm: String): String {
        return Stream.of(*searchTerm.split(" ").toTypedArray())
                .map { term: String -> if (term.endsWith("~") || term.endsWith("\"")) term else "$term~" }
                .collect(Collectors.joining(" "))
    }

    private fun saveObjectToElasticsearch(index: String?, type: String?, obj: Any, id: String): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        try {
            val s = ObjectMappers.getJsonMapper().writeValueAsString(obj)
            client!!.prepareIndex(index, type, id)
                    .setSource(s, XContentType.JSON)
                    .execute().actionGet()
            return true
        } catch (e: JsonProcessingException) {
            logger.debug("error saving object to elasticsearch")
        }
        return false
    }

    private fun deleteObjectById(index: String?, type: String?, id: String): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }
        client!!.prepareDelete(index, type, id).execute().actionGet()
        return true
    }

    private fun mustMatchQuery(field: String, value: Any): MatchQueryBuilder {
        return QueryBuilders.matchQuery(field, value).operator(Operator.AND)
    }

    @SuppressFBWarnings(
            value = ["NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"], justification = "propertyTypeId cannot be " +
            "null"
    )
    private fun buildSort(sortDefinition: SortDefinition): SortBuilder<*> {
        val sort: SortBuilder<*> = when (sortDefinition.sortType) {
            SortType.field -> FieldSortBuilder(getFieldName(sortDefinition.propertyTypeId))
                    .setNestedSort(NestedSortBuilder(ConductorElasticsearchApi.ENTITY))
            SortType.geoDistance -> GeoDistanceSortBuilder(
                    getFieldName(sortDefinition.propertyTypeId),
                    sortDefinition.latitude.get(),
                    sortDefinition.longitude.get()
            )
                    .setNestedSort(NestedSortBuilder(ConductorElasticsearchApi.ENTITY))
            SortType.score -> ScoreSortBuilder()
            else -> ScoreSortBuilder()
        }
        sort.order(if (sortDefinition.descending) SortOrder.DESC else SortOrder.ASC)
        return sort
    }

    private fun getFieldsMap(objectType: SecurableObjectType): Map<String, Float> {
        val f = 1f
        val fieldsMap: MutableMap<String, Float> = Maps.newHashMap()
        val fields: MutableList<String> = Lists.newArrayList(
                SerializationConstants.ID_FIELD,
                SerializationConstants.TITLE_FIELD,
                SerializationConstants.DESCRIPTION_FIELD
        )
        when (objectType) {
            SecurableObjectType.AssociationType -> {
                fields.add(
                        SerializationConstants.ENTITY_TYPE + "." + SerializationConstants.TYPE_FIELD + "."
                                + SerializationConstants.NAME
                )
                fields.add(
                        SerializationConstants.ENTITY_TYPE + "." + SerializationConstants.TYPE_FIELD + "."
                                + SerializationConstants.NAMESPACE
                )
            }
            SecurableObjectType.App -> {
                fields.add(SerializationConstants.NAME)
                fields.add(SerializationConstants.URL)
            }
            SecurableObjectType.EntityTypeCollection -> {
                fields.add(SerializationConstants.TEMPLATE)
                fields.add(SerializationConstants.TYPE_FIELD + "." + SerializationConstants.NAME)
                fields.add(SerializationConstants.TYPE_FIELD + "." + SerializationConstants.NAMESPACE)
                fields.add(SerializationConstants.TEMPLATE + "." + SerializationConstants.ID_FIELD)
                fields.add(SerializationConstants.TEMPLATE + "." + SerializationConstants.NAME_FIELD)
                fields.add(SerializationConstants.TEMPLATE + "." + SerializationConstants.TITLE_FIELD)
                fields.add(SerializationConstants.TEMPLATE + "." + SerializationConstants.DESCRIPTION_FIELD)
            }
            SecurableObjectType.EntitySetCollection -> {
                fields.add(SerializationConstants.ENTITY_TYPE_COLLECTION_ID)
                fields.add(SerializationConstants.NAME_FIELD)
                fields.add(SerializationConstants.CONTACTS)
                fields.add(SerializationConstants.TEMPLATE)
                fields.add(SerializationConstants.ORGANIZATION_ID)
            }
            else -> {
                fields.add(SerializationConstants.TYPE_FIELD + "." + SerializationConstants.NAME)
                fields.add(SerializationConstants.TYPE_FIELD + "." + SerializationConstants.NAMESPACE)
            }
        }
        fields.forEach(Consumer { field: String -> fieldsMap[field] = f })
        return fieldsMap
    }

    //TODO: Make sure these really have to be null paramters.
    fun triggerIndex(
            index: String?,
            type: String?,
            objects: Iterable<*>,
            idFn: Function<Any, String>
    ): Boolean {
        if (!verifyElasticsearchConnection()) {
            return false
        }

        val bulkRequest = client.prepareBulk()

        client.admin().indices().delete(DeleteIndexRequest(index)).actionGet()

        createIndex(index)
        objects.forEach {
            try {
                val id = idFn.apply(it!!)
                val s = ObjectMappers.getJsonMapper().writeValueAsString(it)
                bulkRequest
                        .add(
                                client.prepareIndex(index, type, id)
                                        .setSource(s, XContentType.JSON)
                        )
            } catch (e: JsonProcessingException) {
                logger.error("Error re-indexing securable object type to index {}", index)
            }
        }

        val bulkResponse = bulkRequest.get()
        if (bulkResponse.hasFailures()) {
            bulkResponse.forEach(Consumer { item: BulkItemResponse ->
                logger
                        .error("Failure during attempted re-index: {}", item.failureMessage)
            })
        }
        return true
    }

    private fun getOrganizationObject(organization: Organization): Map<String, Any>? {
        return mapOf(
                SerializationConstants.ID_FIELD to organization.id,
                SerializationConstants.TITLE_FIELD to organization.title,
                SerializationConstants.DESCRIPTION_FIELD to organization.description
        )
    }

    fun verifyElasticsearchConnection(): Boolean {
        if (connected) {
            if (!ElasticsearchTransportClientFactory.isConnected(client)) {
                connected = false
            }
        } else {
            client = factory.client
            if (client != null) {
                connected = true
            }
        }
        return connected
    }

    @Scheduled(fixedRate = 1_800_000)
    @Throws(UnknownHostException::class)
    fun verifyRunner() {
        verifyElasticsearchConnection()
    }

}
