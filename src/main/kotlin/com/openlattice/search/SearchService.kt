package com.openlattice.search

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.annotation.Timed
import com.google.common.base.Stopwatch
import com.google.common.collect.*
import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import com.openlattice.IdConstants
import com.openlattice.apps.App
import com.openlattice.apps.AppType
import com.openlattice.authorization.*
import com.openlattice.authorization.EdmAuthorizationHelper.READ_PERMISSION
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.conductor.rpc.ConductorElasticsearchApi
import com.openlattice.data.DeleteType
import com.openlattice.data.events.EntitiesDeletedEvent
import com.openlattice.data.events.EntitiesUpsertedEvent
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.data.requests.NeighborEntityIds
import com.openlattice.data.storage.EntityDatastore
import com.openlattice.data.storage.IndexingMetadataManager
import com.openlattice.data.storage.MetadataOption
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EdmConstants
import com.openlattice.edm.EntitySet
import com.openlattice.edm.events.*
import com.openlattice.edm.type.AssociationType
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.core.GraphService
import com.openlattice.graph.edge.Edge
import com.openlattice.organizations.Organization
import com.openlattice.organizations.events.OrganizationCreatedEvent
import com.openlattice.organizations.events.OrganizationDeletedEvent
import com.openlattice.organizations.events.OrganizationUpdatedEvent
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet
import com.openlattice.search.requests.*
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors
import kotlin.streams.toList

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Service
class SearchService(
        val eventBus: EventBus,
        val metricRegistry: MetricRegistry,
        val authorizations: AuthorizationManager,
        val elasticsearchApi: ConductorElasticsearchApi,
        val dataModelService: EdmManager,
        val entitySetService: EntitySetManager,
        val graphService: GraphService,
        val dataManager: EntityDatastore,
        val indexingMetadataManager: IndexingMetadataManager
) {

    companion object {
        private val logger = LoggerFactory.getLogger(SearchService::class.java)

        @JvmStatic
        fun getEntityKeyId(entity: Map<FullQualifiedName, Set<Any>>): UUID {
            return UUID.fromString(entity.getValue(EdmConstants.ID_FQN).first().toString())
        }
    }

    init {
        eventBus.register(this)
    }

    private val indexEntitiesTimer = metricRegistry.timer(
            MetricRegistry.name(SearchService::class.java, "indexEntities")
    )

    private val deleteEntitiesTimer = metricRegistry.timer(
            MetricRegistry.name(SearchService::class.java, "deleteEntities")
    )

    private val deleteEntitySetDataTimer = metricRegistry.timer(
            MetricRegistry.name(SearchService::class.java, "entitySetDataCleared")
    )

    private val markAsIndexedTimer = metricRegistry.timer(
            MetricRegistry.name(SearchService::class.java, "markAsIndexed")
    )


    @Timed
    fun executeEntitySetKeywordSearchQuery(
            optionalQuery: Optional<String>,
            optionalEntityType: Optional<UUID>,
            optionalPropertyTypes: Optional<Set<UUID>>,
            start: Int,
            maxHits: Int
    ): SearchResult {

        val authorizedEntitySetIds = authorizations
                .getAuthorizedObjectsOfType(
                        Principals.getCurrentPrincipals(),
                        SecurableObjectType.EntitySet,
                        READ_PERMISSION
                ).collect(Collectors.toSet())

        return if (authorizedEntitySetIds.size == 0) {
            SearchResult(0, arrayListOf())
        } else {
            elasticsearchApi.executeEntitySetMetadataSearch(
                    optionalQuery,
                    optionalEntityType,
                    optionalPropertyTypes,
                    authorizedEntitySetIds,
                    start,
                    maxHits
            )
        }

    }

    @Timed
    fun executeEntitySetCollectionQuery(
            searchTerm: String,
            start: Int,
            maxHits: Int
    ): SearchResult {

        val authorizedEntitySetCollectionIds = authorizations.getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                SecurableObjectType.EntitySetCollection,
                READ_PERMISSION
        ).collect(Collectors.toSet())

        return if (authorizedEntitySetCollectionIds.size == 0) {
            SearchResult(0, Lists.newArrayList())
        } else {
            elasticsearchApi.executeEntitySetCollectionSearch(
                    searchTerm,
                    authorizedEntitySetCollectionIds,
                    start,
                    maxHits
            )
        }

    }

    @Timed
    fun executeSearch(
            searchConstraints: SearchConstraints,
            authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ): DataSearchResult {

        val entitySetIds = searchConstraints.entitySetIds.toSet()
        val entitySetsById = entitySetService.getEntitySetsAsMap(entitySetIds)
        val linkingEntitySets = entitySetsById.values
                .filter { it.isLinking }
                .associate { it.id to DelegatedUUIDSet.wrap(it.linkedEntitySets) }

        val authorizedPropertiesByEntitySet = authorizedPropertyTypesByEntitySet
                .entries
                .associate { it.key to DelegatedUUIDSet.wrap(it.value.keys) }

        if (authorizedPropertiesByEntitySet.isEmpty()) {
            return DataSearchResult(0, Lists.newArrayList())
        }

        val entityTypesByEntitySet = entitySetService
                .getEntitySetsAsMap(searchConstraints.entitySetIds.toSet())
                .mapValues { it.value.entityTypeId }

        val result = elasticsearchApi.executeSearch(
                searchConstraints,
                entityTypesByEntitySet,
                authorizedPropertiesByEntitySet,
                linkingEntitySets
        )

        val entityKeyIdsByEntitySetId = HashMultimap.create<UUID, UUID>()
        result.entityDataKeys
                .forEach { edk -> entityKeyIdsByEntitySetId.put(edk.entitySetId, edk.entityKeyId) }

        //TODO: Properly parallelize this at some point
        val entitiesById = entityKeyIdsByEntitySetId.keySet()
                .parallelStream()
                .map { entitySetId ->
                    val es = entitySetsById.getValue(entitySetId)
                    getResults(
                            es,
                            entityKeyIdsByEntitySetId.get(entitySetId),
                            authorizedPropertyTypesByEntitySet,
                            es.isLinking
                    )
                }
                .flatMap { it.map { entity -> getEntityKeyId(entity) to entity }.stream() }
                .toList()
                .toMap()

        val results = result.entityDataKeys.mapNotNull { entitiesById[it.entityKeyId] }

        return DataSearchResult(result.numHits, results)
    }

    @Timed
    @Subscribe
    fun createEntitySet(event: EntitySetCreatedEvent) {
        val entityType = dataModelService.getEntityType(event.entitySet.entityTypeId)
        elasticsearchApi.saveEntitySetToElasticsearch(entityType, event.entitySet, event.propertyTypes)
    }

    @Timed
    @Subscribe
    fun deleteEntitySet(event: EntitySetDeletedEvent) {
        elasticsearchApi.deleteEntitySet(event.entitySetId, event.entityTypeId)
    }

    @Subscribe
    fun deleteEntities(event: EntitiesDeletedEvent) {
        val deleteEntitiesContext = deleteEntitiesTimer.time()
        val entityTypeId = entitySetService.getEntityTypeByEntitySetId(event.entitySetId).id
        val entitiesDeleted = elasticsearchApi.deleteEntityDataBulk(entityTypeId, event.entityKeyIds)
        deleteEntitiesContext.stop()

        if (entitiesDeleted) {
            // mark them as (un)indexed:
            // - set last_index to now() when it's a hard delete (it does not matter when we
            // get last_write, because the only action take here is to remove the documents)
            // - let background task take soft deletes
            if (event.deleteType === DeleteType.Hard) {
                val markAsIndexedContext = markAsIndexedTimer.time()
                indexingMetadataManager.markAsUnIndexed(mapOf(event.entitySetId to event.entityKeyIds))
                markAsIndexedContext.stop()
            }
        }
    }

    @Subscribe
    fun entitySetDataCleared(event: EntitySetDataDeletedEvent) {
        val deleteEntitySetDataContext = deleteEntitySetDataTimer.time()
        val entityTypeId = entitySetService.getEntityTypeByEntitySetId(event.entitySetId).id
        val entitySetDataDeleted = elasticsearchApi.clearEntitySetData(event.entitySetId, entityTypeId)
        deleteEntitySetDataContext.stop()

        if (entitySetDataDeleted) {
            // mark them as (un)indexed:
            // - set last_index to now() when it's a hard delete (it does not matter when we
            // get last_write, because the only action take here is to remove the documents)
            // - let background task take soft deletes (would be too much overhead for clear calls)
            if (event.deleteType === DeleteType.Hard) {
                val markAsIndexedContext = markAsIndexedTimer.time()
                indexingMetadataManager.markAsUnIndexed(event.entitySetId)
                markAsIndexedContext.stop()
            }
        }
    }

    @Timed
    @Subscribe
    fun createOrganization(event: OrganizationCreatedEvent) {
        elasticsearchApi.createOrganization(event.organization)
    }

    @Timed
    fun executeOrganizationKeywordSearch(searchTerm: SearchTerm): SearchResult {
        val authorizedOrganizationIds = authorizations.getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                SecurableObjectType.Organization,
                READ_PERMISSION
        ).collect(Collectors.toSet())

        return if (authorizedOrganizationIds.size == 0) {
            SearchResult(0, Lists.newArrayList())
        } else elasticsearchApi.executeOrganizationSearch(
                searchTerm.searchTerm,
                authorizedOrganizationIds,
                searchTerm.start,
                searchTerm.maxHits
        )

    }

    @Timed
    @Subscribe
    fun updateOrganization(event: OrganizationUpdatedEvent) {
        elasticsearchApi.updateOrganization(
                event.id,
                event.optionalTitle,
                event.optionalDescription
        )
    }

    @Subscribe
    fun deleteOrganization(event: OrganizationDeletedEvent) {
        elasticsearchApi
                .deleteSecurableObjectFromElasticsearch(SecurableObjectType.Organization, event.organizationId)
    }

    /**
     * Handles when entities are created or updated.
     * In both cases it is enough to re-index the document, ElasticSearch will mark the old document as deleted.
     */
    @Subscribe
    fun indexEntities(event: EntitiesUpsertedEvent) {
        val indexEntitiesContext = indexEntitiesTimer.time()
        val entityTypeId = entitySetService.getEntityTypeByEntitySetId(event.entitySetId).id
        val entitiesIndexed = elasticsearchApi
                .createBulkEntityData(entityTypeId, event.entitySetId, event.entities)
        indexEntitiesContext.stop()

        if (entitiesIndexed) {
            val markAsIndexedContext = markAsIndexedTimer.time()
            val lastWrites = event.entities.mapValues {
                it.value.getValue(IdConstants.LAST_WRITE_ID.id).first() as OffsetDateTime
            }
            // mark them as indexed

            indexingMetadataManager.markAsIndexed(mapOf(event.entitySetId to lastWrites))
            markAsIndexedContext.stop()
        }
    }

    @Subscribe
    fun updateEntitySetMetadata(event: EntitySetMetadataUpdatedEvent) {
        elasticsearchApi.updateEntitySetMetadata(event.entitySet)
    }

    @Subscribe
    fun updatePropertyTypesInEntitySet(event: PropertyTypesInEntitySetUpdatedEvent) {
        elasticsearchApi.updatePropertyTypesInEntitySet(event.entitySetId, event.updatedPropertyTypes)
    }

    /**
     * If 1 or more property types are added to an entity type, the corresponding mapping needs to be updated
     */
    @Subscribe
    fun addPropertyTypesToEntityType(event: PropertyTypesAddedToEntityTypeEvent) {
        elasticsearchApi.addPropertyTypesToEntityType(event.entityType, event.newPropertyTypes)
    }

    @Subscribe
    fun createEntityType(event: EntityTypeCreatedEvent) {
        val entityType = event.entityType
        val propertyTypes = Lists
                .newArrayList(dataModelService.getPropertyTypes(entityType.properties))
        elasticsearchApi.saveEntityTypeToElasticsearch(entityType, propertyTypes)
    }

    @Subscribe
    fun createAssociationType(event: AssociationTypeCreatedEvent) {
        val associationType = event.associationType
        val propertyTypes = Lists
                .newArrayList(
                        dataModelService
                                .getPropertyTypes(associationType.associationEntityType.properties)
                )
        elasticsearchApi.saveAssociationTypeToElasticsearch(associationType, propertyTypes)
    }

    @Subscribe
    fun createPropertyType(event: PropertyTypeCreatedEvent) {
        val propertyType = event.propertyType
        elasticsearchApi
                .saveSecurableObjectToElasticsearch(SecurableObjectType.PropertyTypeInEntitySet, propertyType)
    }

    @Subscribe
    fun createApp(event: AppCreatedEvent) {
        val app = event.app
        elasticsearchApi.saveSecurableObjectToElasticsearch(SecurableObjectType.App, app)
    }

    @Subscribe
    fun createAppType(event: AppTypeCreatedEvent) {
        val appType = event.appType
        elasticsearchApi.saveSecurableObjectToElasticsearch(SecurableObjectType.AppType, appType)
    }

    @Subscribe
    fun deleteEntityType(event: EntityTypeDeletedEvent) {
        val entityTypeId = event.entityTypeId
        elasticsearchApi.deleteSecurableObjectFromElasticsearch(SecurableObjectType.EntityType, entityTypeId)
    }

    @Subscribe
    fun deleteAssociationType(event: AssociationTypeDeletedEvent) {
        val associationTypeId = event.associationTypeId
        elasticsearchApi
                .deleteSecurableObjectFromElasticsearch(SecurableObjectType.AssociationType, associationTypeId)
    }

    @Subscribe
    fun createEntityTypeCollection(event: EntityTypeCollectionCreatedEvent) {
        val entityTypeCollection = event.entityTypeCollection
        elasticsearchApi
                .saveSecurableObjectToElasticsearch(SecurableObjectType.EntityTypeCollection, entityTypeCollection)
    }

    @Subscribe
    fun deleteEntityTypeCollection(event: EntityTypeCollectionDeletedEvent) {
        val entityTypeCollectionId = event.entityTypeCollectionId
        elasticsearchApi
                .deleteSecurableObjectFromElasticsearch(
                        SecurableObjectType.EntityTypeCollection,
                        entityTypeCollectionId
                )
    }

    @Subscribe
    fun createEntitySetCollection(event: EntitySetCollectionCreatedEvent) {
        val entitySetCollection = event.entitySetCollection
        elasticsearchApi
                .saveSecurableObjectToElasticsearch(SecurableObjectType.EntitySetCollection, entitySetCollection)
    }

    @Subscribe
    fun deleteEntitySetCollection(event: EntitySetCollectionDeletedEvent) {
        val entitySetCollectionId = event.entitySetCollectionId
        elasticsearchApi
                .deleteSecurableObjectFromElasticsearch(
                        SecurableObjectType.EntitySetCollection,
                        entitySetCollectionId
                )
    }

    /**
     * Handle deleting the index for that property type.
     * At this point, none of the entity sets should contain this property type anymore, so the entity set data mappings
     * are not affected.
     */
    @Subscribe
    fun deletePropertyType(event: PropertyTypeDeletedEvent) {
        val propertyTypeId = event.propertyTypeId
        elasticsearchApi
                .deleteSecurableObjectFromElasticsearch(SecurableObjectType.PropertyTypeInEntitySet, propertyTypeId)
    }

    @Subscribe
    fun deleteApp(event: AppDeletedEvent) {
        val appId = event.appId
        elasticsearchApi.deleteSecurableObjectFromElasticsearch(SecurableObjectType.App, appId)
    }

    @Subscribe
    fun deleteAppType(event: AppTypeDeletedEvent) {
        val appTypeId = event.appTypeId
        elasticsearchApi.deleteSecurableObjectFromElasticsearch(SecurableObjectType.AppType, appTypeId)
    }

    fun executeEntityTypeSearch(searchTerm: String, start: Int, maxHits: Int): SearchResult {
        return elasticsearchApi
                .executeSecurableObjectSearch(SecurableObjectType.EntityType, searchTerm, start, maxHits)
    }

    fun executeAssociationTypeSearch(searchTerm: String, start: Int, maxHits: Int): SearchResult {
        return elasticsearchApi
                .executeSecurableObjectSearch(SecurableObjectType.AssociationType, searchTerm, start, maxHits)
    }

    fun executePropertyTypeSearch(searchTerm: String, start: Int, maxHits: Int): SearchResult {
        return elasticsearchApi.executeSecurableObjectSearch(
                SecurableObjectType.PropertyTypeInEntitySet,
                searchTerm,
                start,
                maxHits
        )
    }

    fun executeAppSearch(searchTerm: String, start: Int, maxHits: Int): SearchResult {
        return elasticsearchApi.executeSecurableObjectSearch(SecurableObjectType.App, searchTerm, start, maxHits)
    }

    fun executeEntityTypeCollectionSearch(searchTerm: String, start: Int, maxHits: Int): SearchResult {
        return elasticsearchApi.executeSecurableObjectSearch(
                SecurableObjectType.EntityTypeCollection,
                searchTerm,
                start,
                maxHits
        )
    }

    fun executeFQNEntityTypeSearch(namespace: String, name: String, start: Int, maxHits: Int): SearchResult {
        return elasticsearchApi
                .executeSecurableObjectFQNSearch(SecurableObjectType.EntityType, namespace, name, start, maxHits)
    }

    fun executeFQNPropertyTypeSearch(namespace: String, name: String, start: Int, maxHits: Int): SearchResult {
        return elasticsearchApi.executeSecurableObjectFQNSearch(
                SecurableObjectType.PropertyTypeInEntitySet,
                namespace,
                name,
                start,
                maxHits
        )
    }

    @Timed
    fun executeEntityNeighborSearch(
            entitySetIds: Set<UUID>,
            filter: EntityNeighborsFilter,
            principals: Set<Principal>
    ): Map<UUID, List<NeighborEntityDetails>> {
        val sw1 = Stopwatch.createStarted()
        val sw2 = Stopwatch.createStarted()

        logger.debug("Starting Entity Neighbor Search...")
        if (filter.associationEntitySetIds.isPresent && filter.associationEntitySetIds.get().isEmpty()) {
            logger.debug("Missing association entity set ids.. returning empty result")
            return ImmutableMap.of()
        }

        val linkingEntitySets = entitySetService
                .getEntitySetsAsMap(entitySetIds)
                .values
                .filter { !it.isLinking }

        var entityKeyIdsByLinkingId: Map<UUID, Set<UUID>> = ImmutableMap.of()

        val entityKeyIds = Sets.newHashSet(filter.entityKeyIds)
        val allBaseEntitySetIds = Sets.newHashSet(entitySetIds)

        if (linkingEntitySets.isNotEmpty()) {
            val normalEntitySetIds = linkingEntitySets.flatMap { it.linkedEntitySets }.toSet()

            entityKeyIdsByLinkingId = getEntityKeyIdsByLinkingIds(entityKeyIds, normalEntitySetIds)
            entityKeyIdsByLinkingId.values.forEach { entityKeyIds.addAll(it) }
            entityKeyIds.removeAll(entityKeyIdsByLinkingId.keys) // remove linking ids

            //TODO: This like an odd spot to place this general logic.
            // normal entity sets within 1 linking entity set are only authorized if all of them is authorized
            val authorizedNormalEntitySetIds = linkingEntitySets
                    .map { it.linkedEntitySets }
                    .filter { esIds ->
                        esIds.all { esId ->
                            authorizations.checkIfHasPermissions(AclKey(esId), principals, READ_PERMISSION)
                        }
                    }.flatten().toSet()

            allBaseEntitySetIds.addAll(authorizedNormalEntitySetIds)
        }

        val edges = Lists.newArrayList<Edge>()
        val allEntitySetIds = Sets.newHashSet<UUID>()
        val authorizedEdgeESIdsToVertexESIds = Maps.newHashMap<UUID, MutableSet<UUID>>()
        val entitySetIdToEntityKeyId = HashMultimap.create<UUID, UUID>()
        val entitySetsIdsToAuthorizedProps = mutableMapOf<UUID, MutableMap<UUID, PropertyType>>()

        graphService.getEdgesAndNeighborsForVerticesBulk(
                allBaseEntitySetIds,
                EntityNeighborsFilter(
                        entityKeyIds,
                        filter.srcEntitySetIds,
                        filter.dstEntitySetIds,
                        filter.associationEntitySetIds
                )
        ).forEach { edge ->
            edges.add(edge)
            allEntitySetIds.add(edge.edge.entitySetId)
            allEntitySetIds.add(
                    if (entityKeyIds.contains(edge.src.entityKeyId))
                        edge.dst.entitySetId
                    else
                        edge.src.entitySetId
            )
        }
        logger.debug(
                "Get edges and neighbors for vertices query for {} ids finished in {} ms",
                filter.entityKeyIds.size,
                sw1.elapsed(TimeUnit.MILLISECONDS)
        )
        sw1.reset().start()

        val authorizedEntitySetIds = authorizations
                .accessChecksForPrincipals(
                        allEntitySetIds.map { esId -> AccessCheck(AclKey(esId), READ_PERMISSION) }.toSet(),
                        principals
                )
                .filter { auth -> auth.permissions.getValue(Permission.READ) }
                .map { auth -> auth.aclKey.first() }
                .collect(Collectors.toSet())

        val entitySetsById = entitySetService.getEntitySetsAsMap(authorizedEntitySetIds)

        val entityTypesById = dataModelService
                .getEntityTypesAsMap(entitySetsById.values.map { entitySet ->
                    entitySetsIdsToAuthorizedProps[entitySet.id] = mutableMapOf()
                    authorizedEdgeESIdsToVertexESIds[entitySet.id] = mutableSetOf()
                    entitySet.entityTypeId
                }.toSet())

        val propertyTypesById = dataModelService
                .getPropertyTypesAsMap(entityTypesById.values.flatMap { it.properties }.toSet())

        val accessChecks = entitySetsById.values
                .flatMap { entitySet ->
                    entityTypesById
                            .getValue(entitySet.entityTypeId)
                            .properties
                            .map { propertyTypeId ->
                                AccessCheck(
                                        AclKey(entitySet.getId(), propertyTypeId),
                                        READ_PERMISSION
                                )
                            }
                }.toSet()

        authorizations
                .accessChecksForPrincipals(accessChecks, principals)
                .forEach { auth ->
                    if (auth.permissions.getValue(Permission.READ)) {
                        val esId = auth.aclKey[0]
                        val propertyTypeId = auth.aclKey[1]
                        entitySetsIdsToAuthorizedProps
                                .getValue(esId)[propertyTypeId] = propertyTypesById.getValue(propertyTypeId)
                    }
                }

        logger.debug(
                "Access checks for entity sets and their properties finished in {} ms",
                sw1.elapsed(TimeUnit.MILLISECONDS)
        )

        sw1.reset().start()

        edges.forEach { edge ->
            val edgeEntityKeyId = edge.edge.entityKeyId
            val neighborEntityKeyId = if ((entityKeyIds.contains(edge.src.entityKeyId)))
                edge.dst.entityKeyId
            else
                edge.src.entityKeyId
            val edgeEntitySetId = edge.edge.entitySetId
            val neighborEntitySetId = if ((entityKeyIds.contains(edge.src.entityKeyId)))
                edge.dst.entitySetId
            else
                edge.src.entitySetId

            if (entitySetsIdsToAuthorizedProps.containsKey(edgeEntitySetId)) {
                entitySetIdToEntityKeyId.put(edgeEntitySetId, edgeEntityKeyId)

                if (entitySetsIdsToAuthorizedProps.containsKey(neighborEntitySetId)) {
                    authorizedEdgeESIdsToVertexESIds.getValue(edgeEntitySetId).add(neighborEntitySetId)
                    entitySetIdToEntityKeyId.put(neighborEntitySetId, neighborEntityKeyId)
                }
            }

        }
        logger.debug("Edge and neighbor entity key ids collected in {} ms", sw1.elapsed(TimeUnit.MILLISECONDS))
        sw1.reset().start()

        val entitiesByEntitySetId = dataManager
                .getEntitiesAcrossEntitySets(entitySetIdToEntityKeyId, entitySetsIdsToAuthorizedProps)
        logger.debug("Get entities across entity sets query finished in {} ms", sw1.elapsed(TimeUnit.MILLISECONDS))
        sw1.reset().start()

        val entities = Maps.newHashMap<UUID, Map<FullQualifiedName, Set<Any>>>()
        entitiesByEntitySetId.values.forEach { entries ->
            entries.forEach { entry ->
                entities.put(
                        getEntityKeyId(entry), entry
                )
            }
        }

        val entityNeighbors = Maps.newConcurrentMap<UUID, MutableList<NeighborEntityDetails>>()

        // create a NeighborEntityDetails object for each edge based on authorizations
        edges.stream().forEach { edge ->
            val vertexIsSrc = entityKeyIds.contains(edge.key.src.entityKeyId)
            val entityId = if ((vertexIsSrc))
                edge.key.src.entityKeyId
            else
                edge.key.dst.entityKeyId
            if (!entityNeighbors.containsKey(entityId)) {
                entityNeighbors.put(
                        entityId, Collections.synchronizedList(
                        Lists.newArrayList()
                )
                )
            }
            val neighbor = getNeighborEntityDetails(
                    edge,
                    authorizedEdgeESIdsToVertexESIds,
                    entitySetsById,
                    vertexIsSrc,
                    entities
            )
            if (neighbor != null) {
                entityNeighbors.getValue(entityId).add(neighbor)
            }
        }
        logger.debug("Neighbor entity details collected in {} ms", sw1.elapsed(TimeUnit.MILLISECONDS))

        /* Map linkingIds to the collection of neighbors for all entityKeyIds in the cluster */
        entityKeyIdsByLinkingId.forEach { (linkingId, normalEntityKeyIds) ->
            entityNeighbors[linkingId] = normalEntityKeyIds
                    .flatMap { entityKeyId ->
                        entityNeighbors.getOrDefault(entityKeyId, arrayListOf())
                    }.toMutableList()

        }

        entityNeighbors.entries
                .removeIf { entry -> !filter.entityKeyIds.contains(entry.key) }

        logger.debug("Finished entity neighbor search in {} ms", sw2.elapsed(TimeUnit.MILLISECONDS))
        return entityNeighbors
    }

    private fun getNeighborEntityDetails(
            edge: Edge,
            authorizedEdgeESIdsToVertexESIds: Map<UUID, Set<UUID>>,
            entitySetsById: Map<UUID, EntitySet>,
            vertexIsSrc: Boolean,
            entities: Map<UUID, Map<FullQualifiedName, Set<Any>>>
    ): NeighborEntityDetails? {

        val edgeEntitySetId = edge.edge.entitySetId
        if (authorizedEdgeESIdsToVertexESIds.containsKey(edgeEntitySetId)) {
            val neighborEntityKeyId = if (vertexIsSrc)
                edge.dst.entityKeyId
            else
                edge.src.entityKeyId
            val neighborEntitySetId = if (vertexIsSrc)
                edge.dst.entitySetId
            else
                edge.src.entitySetId

            val edgeDetails = entities[edge.edge.entityKeyId]
            if (edgeDetails != null) {
                if (authorizedEdgeESIdsToVertexESIds.getValue(edgeEntitySetId).contains(neighborEntitySetId)) {
                    val neighborDetails = entities[neighborEntityKeyId]

                    if (neighborDetails != null) {
                        return NeighborEntityDetails(
                                entitySetsById[edgeEntitySetId],
                                edgeDetails,
                                entitySetsById[neighborEntitySetId],
                                neighborEntityKeyId,
                                neighborDetails,
                                vertexIsSrc
                        )
                    }

                } else {
                    return NeighborEntityDetails(
                            entitySetsById[edgeEntitySetId],
                            edgeDetails,
                            vertexIsSrc
                    )
                }
            }
        }
        return null
    }

    @Timed
    fun executeLinkingEntityNeighborIdsSearch(
            normalEntitySetIds: Set<UUID>,
            filter: EntityNeighborsFilter,
            principals: Set<Principal>
    ): Map<UUID, Map<UUID, SetMultimap<UUID, NeighborEntityIds>>> {
        if (filter.associationEntitySetIds.isPresent && filter.associationEntitySetIds.get().isEmpty()) {
            return ImmutableMap.of()
        }

        val linkingIds = filter.entityKeyIds

        val entityKeyIdsByLinkingIds = getEntityKeyIdsByLinkingIds(linkingIds, normalEntitySetIds)

        val entityKeyIds = entityKeyIdsByLinkingIds.values.flatten().toSet()

        // Will return only entries, where there is at least 1 neighbor
        val entityNeighbors = executeEntityNeighborIdsSearch(
                normalEntitySetIds,
                EntityNeighborsFilter(
                        entityKeyIds,
                        filter.srcEntitySetIds,
                        filter.dstEntitySetIds,
                        filter.associationEntitySetIds
                ),
                principals
        )

        return if (entityNeighbors.isEmpty()) {
            entityNeighbors
        } else entityKeyIdsByLinkingIds.asSequence()
                .filter { entityKeyIdsOfLinkingId ->
                    entityNeighbors.keys.any { entityKeyIdsOfLinkingId.value.contains(it) }
                }
                .map { (linkingId, entityKeyIdsOfLinkingId) ->
                    val neighborIds = mutableMapOf<UUID, SetMultimap<UUID, NeighborEntityIds>>()
                    entityKeyIdsOfLinkingId
                            .asSequence()
                            .filter { entityNeighbors.containsKey(it) }
                            .forEach { entityKeyId ->
                                entityNeighbors.getValue(entityKeyId)
                                        .forEach { (entityKey, neighbors) ->
                                            neighborIds.getOrPut(entityKey) { HashMultimap.create() }.putAll(neighbors)
                                        }
                            }
                    linkingId to neighborIds
                }.toMap()
    }

    @Timed
    fun  executeEntityNeighborIdsSearch(
            entitySetIds: Set<UUID>,
            filter: EntityNeighborsFilter,
            principals: Set<Principal>
    ): Map<UUID, Map<UUID, SetMultimap<UUID, NeighborEntityIds>>> {
        val sw1 = Stopwatch.createStarted()

        logger.info("Starting Reduced Entity Neighbor Search...")
        if (filter.associationEntitySetIds.isPresent && filter.associationEntitySetIds.get().isEmpty()) {
            logger.info("Missing association entity set ids. Returning empty result.")
            return ImmutableMap.of()
        }

        val entityKeyIds = filter.entityKeyIds
        val allEntitySetIds = Sets.newHashSet<UUID>()

        val neighbors = mutableMapOf<UUID, MutableMap<UUID, SetMultimap<UUID, NeighborEntityIds>>>()

        graphService.getEdgesAndNeighborsForVerticesBulk(entitySetIds, filter).forEach { edge ->

            val isSrc = entityKeyIds.contains(edge.src.entityKeyId)
            val entityKeyId = if (isSrc) edge.src.entityKeyId else edge.dst.entityKeyId
            val neighborEntityDataKey = if (isSrc) edge.dst else edge.src

            val neighborEntityIds = NeighborEntityIds(
                    edge.edge.entityKeyId,
                    neighborEntityDataKey.entityKeyId,
                    isSrc
            )

            neighbors
                    .getOrPut(entityKeyId) { mutableMapOf() }
                    .getOrPut(edge.edge.entitySetId) { HashMultimap.create<UUID, NeighborEntityIds>() }
                    .put(neighborEntityDataKey.entitySetId, neighborEntityIds)


            allEntitySetIds.add(edge.edge.entitySetId)
            allEntitySetIds.add(neighborEntityDataKey.entitySetId)

        }

        val unauthorizedEntitySetIds = authorizations
                .accessChecksForPrincipals(
                        allEntitySetIds
                                .map { esId ->
                                    AccessCheck(
                                            AclKey(esId),
                                            READ_PERMISSION
                                    )
                                }.toSet(), principals
                )
                .filter { auth -> !auth.permissions.getValue(Permission.READ) }
                .map { auth -> auth.aclKey[0] }
                .collect(Collectors.toSet())

        if (unauthorizedEntitySetIds.size > 0) {

            neighbors.values.forEach { associationMap ->
                associationMap.values.forEach { neighborsMap ->
                    neighborsMap.entries()
                            .removeIf { neighborEntry -> unauthorizedEntitySetIds.contains(neighborEntry.key) }
                }
                associationMap.entries.removeIf { entry ->
                    (unauthorizedEntitySetIds.contains(
                            entry.key
                    ) || entry.value.size() == 0)
                }

            }

        }

        logger.info("Reduced entity neighbor search took {} ms", sw1.elapsed(TimeUnit.MILLISECONDS))

        return neighbors
    }

    private fun getResults(
            entitySet: EntitySet,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            linking: Boolean
    ): List<Map<FullQualifiedName, Set<Any>>> {
        if (entityKeyIds.isEmpty()) {
            return ImmutableList.of()
        }
        if (linking) {
            val linkingIdsByEntitySetIds = entitySet
                    .linkedEntitySets
                    .associateWith { Optional.of (entityKeyIds) }
            val authorizedPropertiesOfNormalEntitySets = entitySet
                    .linkedEntitySets
                    .associateWith { authorizedPropertyTypes.getValue(entitySet.id) }

            return dataManager.getLinkingEntitiesWithMetadata(
                    linkingIdsByEntitySetIds,
                    authorizedPropertiesOfNormalEntitySets,
                    EnumSet.of(MetadataOption.LAST_WRITE)
            ).toList()
        } else {
            return dataManager
                    .getEntitiesWithMetadata(
                            entitySet.id,
                            ImmutableSet.copyOf(entityKeyIds),
                            authorizedPropertyTypes,
                            EnumSet.of(MetadataOption.LAST_WRITE)
                    )
                    .toList()
        }
    }

    private fun getEntityKeyIdsByLinkingIds(
            linkingIds: Set<UUID>,
            normalEntitySetIds: Set<UUID>
    ): Map<UUID, Set<UUID>> {
        return dataManager.getEntityKeyIdsOfLinkingIds(linkingIds, normalEntitySetIds).toMap()
    }

    fun triggerPropertyTypeIndex(propertyTypes: List<PropertyType>) {
        elasticsearchApi.triggerSecurableObjectIndex(SecurableObjectType.PropertyTypeInEntitySet, propertyTypes)
    }

    fun triggerEntityTypeIndex(entityTypes: List<EntityType>) {
        elasticsearchApi.triggerSecurableObjectIndex(SecurableObjectType.EntityType, entityTypes)
    }

    fun triggerAssociationTypeIndex(associationTypes: List<AssociationType>) {
        elasticsearchApi.triggerSecurableObjectIndex(SecurableObjectType.AssociationType, associationTypes)
    }

    fun triggerEntitySetIndex() {
        val entitySets = entitySetService
                .getEntitySets()
                .associateWith { dataModelService.getEntityType(it.entityTypeId).properties }

        val propertyTypes = dataModelService
                .propertyTypes
                .associateBy { it.id }
        elasticsearchApi.triggerEntitySetIndex(entitySets, propertyTypes)
    }

    fun triggerEntitySetDataIndex(entitySetId: UUID) {
        val entityType = entitySetService.getEntityTypeByEntitySetId(entitySetId)
        val propertyTypes = dataModelService.getPropertyTypesAsMap(entityType.properties)
        val propertyTypeList = Lists.newArrayList(propertyTypes.values)

        elasticsearchApi.deleteEntitySet(entitySetId, entityType.id)
        elasticsearchApi.saveEntitySetToElasticsearch(
                entityType,
                entitySetService.getEntitySet(entitySetId),
                propertyTypeList
        )

        val entitySet = entitySetService.getEntitySet(entitySetId)
        val entitySetIds = if (entitySet!!.isLinking) entitySet.linkedEntitySets else setOf(entitySetId)
        indexingMetadataManager.markEntitySetsAsNeedsToBeIndexed(entitySetIds, entitySet.isLinking)
    }

    fun triggerAllEntitySetDataIndex() {
        entitySetService.getEntitySets().forEach { entitySet -> triggerEntitySetDataIndex(entitySet.getId()) }
    }

    fun triggerAppIndex(apps: List<App>) {
        elasticsearchApi.triggerSecurableObjectIndex(SecurableObjectType.App, apps)
    }

    fun triggerAppTypeIndex(appTypes: List<AppType>) {
        elasticsearchApi.triggerSecurableObjectIndex(SecurableObjectType.AppType, appTypes)
    }

    fun triggerAllOrganizationsIndex(allOrganizations: List<Organization>) {
        elasticsearchApi.triggerOrganizationIndex(allOrganizations)
    }

    fun triggerOrganizationIndex(organization: Organization) {
        elasticsearchApi.triggerOrganizationIndex(Lists.newArrayList(organization))
    }

}

