package com.openlattice.search

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.annotation.Timed
import com.google.common.collect.*
import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import com.openlattice.IdConstants
import com.openlattice.apps.App
import com.openlattice.authorization.*
import com.openlattice.authorization.EdmAuthorizationHelper.READ_PERMISSION
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.conductor.rpc.ConductorElasticsearchApi
import com.openlattice.data.DataEdgeKey
import com.openlattice.data.DeleteType
import com.openlattice.data.EntityDataKey
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
import com.openlattice.graph.NeighborPage
import com.openlattice.graph.PagedNeighborRequest
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
            optionalOrganizationId: Optional<UUID>,
            excludePropertyTypes: Boolean,
            start: Int,
            maxHits: Int
    ): SearchResult {

        var authorizedEntitySetIds = authorizations
                .getAuthorizedObjectsOfType(
                        Principals.getCurrentPrincipals(),
                        SecurableObjectType.EntitySet,
                        READ_PERMISSION
                ).collect(Collectors.toSet())

        if (optionalOrganizationId.isPresent) {
            authorizedEntitySetIds = entitySetService.filterEntitySetsForOrganization(
                    optionalOrganizationId.get(),
                    authorizedEntitySetIds.map { it.first() }
            ).map { AclKey(it) }.toSet()
        }

        return if (authorizedEntitySetIds.size == 0) {
            SearchResult(0, arrayListOf())
        } else {
            elasticsearchApi.executeEntitySetMetadataSearch(
                    optionalQuery,
                    optionalEntityType,
                    optionalPropertyTypes,
                    excludePropertyTypes,
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

    private fun getAuthorizedFilterEntitySetOptions(
            entitySetIds: Set<UUID>,
            filter: EntityNeighborsFilter,
            principals: Set<Principal>
    ): Pair<EntityNeighborsFilter, Map<UUID, Map<UUID, PropertyType>>> {

        val srcEntitySetIds = mutableSetOf<UUID>()
        val dstEntitySetIds = mutableSetOf<UUID>()
        val associationEntitySetIds = mutableSetOf<UUID>()

        graphService.getNeighborEntitySets(entitySetIds).forEach { neighborSet ->
            srcEntitySetIds.add(neighborSet.srcEntitySetId)
            dstEntitySetIds.add(neighborSet.dstEntitySetId)
            associationEntitySetIds.add(neighborSet.edgeEntitySetId)
        }

        val authorizedEntitySetIds = authorizations
                .accessChecksForPrincipals(
                        (srcEntitySetIds + dstEntitySetIds + associationEntitySetIds).map { esId ->
                            AccessCheck(AclKey(esId), READ_PERMISSION)
                        }.toSet(),
                        principals
                )
                .filter { auth -> auth.permissions.getValue(Permission.READ) }
                .map { auth -> auth.aclKey.first() }
                .collect(Collectors.toSet())

        val authorizedPropertyTypesByEntitySet = getAuthorizedPropertyTypesOfEntitySets(authorizedEntitySetIds, principals)

        val srcFilteredEntitySetIds = filter.srcEntitySetIds.orElse(srcEntitySetIds)
                .filter { srcEntitySetIds.contains(it) && authorizedPropertyTypesByEntitySet.contains(it) }.toSet()
        val dstFilteredEntitySetIds = filter.dstEntitySetIds.orElse(dstEntitySetIds)
                .filter { dstEntitySetIds.contains(it) && authorizedPropertyTypesByEntitySet.contains(it) }.toSet()
        val associationFilteredEntitySetIds = filter.associationEntitySetIds.orElse(associationEntitySetIds)
                .filter { associationEntitySetIds.contains(it) && authorizedPropertyTypesByEntitySet.contains(it) }.toSet()

        return EntityNeighborsFilter(
                filter.entityKeyIds,
                Optional.of(srcFilteredEntitySetIds),
                Optional.of(dstFilteredEntitySetIds),
                Optional.of(associationFilteredEntitySetIds)
        ) to authorizedPropertyTypesByEntitySet
    }

    private fun getAuthorizedPropertyTypesOfEntitySets(
            entitySetIds: Set<UUID>,
            principals: Set<Principal>
    ): Map<UUID, Map<UUID, PropertyType>> {

        val entityTypeIdsByEntitySet = entitySetService.getEntityTypeIdsByEntitySetIds(entitySetIds)
        val entityTypesById = dataModelService.getEntityTypesAsMap(entityTypeIdsByEntitySet.values.toSet())

        val propertyTypesById = dataModelService
                .getPropertyTypesAsMap(entityTypesById.values.flatMap { it.properties }.toSet())

        val accessChecks = entityTypeIdsByEntitySet.entries
                .flatMap { (entitySetId, entityTypeId) ->
                    entityTypesById
                            .getValue(entityTypeId)
                            .properties
                            .map { propertyTypeId ->
                                AccessCheck(
                                        AclKey(entitySetId, propertyTypeId),
                                        READ_PERMISSION
                                )
                            }
                }.toSet()

        val entitySetsIdsToAuthorizedProps = mutableMapOf<UUID, MutableMap<UUID, PropertyType>>()

        authorizations
                .accessChecksForPrincipals(accessChecks, principals)
                .forEach { auth ->
                    if (auth.permissions.getValue(Permission.READ)) {
                        val esId = auth.aclKey[0]
                        val propertyTypeId = auth.aclKey[1]
                        entitySetsIdsToAuthorizedProps
                                .getOrPut(esId) { mutableMapOf() }[propertyTypeId] = propertyTypesById.getValue(propertyTypeId)
                    }
                }

        return entitySetsIdsToAuthorizedProps
    }

    private fun getLinkingEntitySets(entitySetIds: Set<UUID>): Map<UUID, EntitySet> {
        return entitySetService
                .getEntitySetsAsMap(entitySetIds)
                .filter { !it.value.isLinking }
    }

    private fun getBaseEntitySetIdsOfLinkingEntitySets(
            linkingEntitySets: Map<UUID, EntitySet>,
            principals: Set<Principal>
    ): Set<UUID> {
        if (linkingEntitySets.isEmpty()) {
            return mutableSetOf()
        }

        return linkingEntitySets
                .values
                .map { it.linkedEntitySets }
                .filter { esIds ->
                    esIds.all { esId ->
                        authorizations.checkIfHasPermissions(AclKey(esId), principals, READ_PERMISSION)
                    }
                }.flatten().toSet()

    }

    private fun getEntityKeyIdsByLinkingId(
            linkingEntitySets: Map<UUID, EntitySet>,
            entityKeyIds: Set<UUID>
    ): Map<UUID, Set<UUID>> {
        if (linkingEntitySets.isEmpty()) {
            return mutableMapOf()
        }

        val normalEntitySetIds = linkingEntitySets.values.flatMap { it.linkedEntitySets }.toSet()

        return getEntityKeyIdsByLinkingIds(entityKeyIds, normalEntitySetIds)
    }

    private fun getEntityKeyIdsToQueryFor(
            entityKeyIdsByLinkingId: Map<UUID, Set<UUID>>,
            filterEntityKeyIds: Set<UUID>
    ): Set<UUID> {
        if (entityKeyIdsByLinkingId.isEmpty()) {
            return filterEntityKeyIds
        }

        val entityKeyIds = filterEntityKeyIds.toMutableSet()

        entityKeyIdsByLinkingId.values.forEach { entityKeyIds.addAll(it) }
        entityKeyIds.removeAll(entityKeyIdsByLinkingId.keys) // remove linking ids

        return entityKeyIds
    }

    private fun getNeighborEntitySetIdToEntityKeyIdForEdges(edges: List<Edge>, entityKeyIds: Set<UUID>): SetMultimap<UUID, UUID> {
        val entitySetIdToEntityKeyId = HashMultimap.create<UUID, UUID>()

        edges.forEach { edge ->
            entitySetIdToEntityKeyId.put(edge.edge.entitySetId, edge.edge.entityKeyId)

            if (entityKeyIds.contains(edge.src.entityKeyId)) {
                entitySetIdToEntityKeyId.put(edge.dst.entitySetId, edge.dst.entityKeyId)
            }

            if (entityKeyIds.contains(edge.dst.entityKeyId)) {
                entitySetIdToEntityKeyId.put(edge.src.entitySetId, edge.src.entityKeyId)
            }
        }

        return entitySetIdToEntityKeyId
    }

    @Timed
    fun executeEntityNeighborSearch(
            entitySetIds: Set<UUID>,
            pagedNeighborRequest: PagedNeighborRequest,
            principals: Set<Principal>
    ): NeighborPage {


        /* Load all possible association/neighbor entity set combos and perform auth checks **/

        val (filter, entitySetsIdsToAuthorizedProps) = getAuthorizedFilterEntitySetOptions(
                entitySetIds,
                pagedNeighborRequest.filter,
                principals
        )
        val authorizedPagedNeighborRequest = PagedNeighborRequest(filter, pagedNeighborRequest.bookmark, pagedNeighborRequest.pageSize)

        val allEntitySets = filter.srcEntitySetIds.get() + filter.dstEntitySetIds.get() + filter.associationEntitySetIds.get()

        val entitySetsById = entitySetService.getEntitySetsAsMap(allEntitySets)

        if (filter.associationEntitySetIds.isPresent && filter.associationEntitySetIds.get().isEmpty()) {
            logger.info("Missing association entity set ids.. returning empty result")
            return NeighborPage(linkedMapOf(), null)
        }


        /* Handle linking entity sets, if present */

        val linkingEntitySets = getLinkingEntitySets(entitySetIds)

        val allBaseEntitySetIds = entitySetIds + getBaseEntitySetIdsOfLinkingEntitySets(linkingEntitySets, principals)
        val entityKeyIdsByLinkingId = getEntityKeyIdsByLinkingId(linkingEntitySets, filter.entityKeyIds)
        val entityKeyIds = getEntityKeyIdsToQueryFor(entityKeyIdsByLinkingId, filter.entityKeyIds)


        /* Load authorized edges and their corresponding neighbor data */

        val edges = graphService.getEdgesAndNeighborsForVertices(allBaseEntitySetIds, authorizedPagedNeighborRequest).toList()

        val entitySetIdToEntityKeyId = getNeighborEntitySetIdToEntityKeyIdForEdges(edges, entityKeyIds)

        val entitiesByEntitySetId = dataManager
                .getEntitiesAcrossEntitySets(entitySetIdToEntityKeyId, entitySetsIdsToAuthorizedProps)

        val entities = Maps.newHashMap<UUID, Map<FullQualifiedName, Set<Any>>>()
        entitiesByEntitySetId.values.forEach { entries ->
            entries.forEach { entry ->
                entities[getEntityKeyId(entry)] = entry
            }
        }

        /* Format neighbor data into the expected return format */
        val entityNeighbors = Maps.newLinkedHashMap<UUID, MutableList<NeighborEntityDetails>>()

        edges.forEach { edge ->

            mapOf(
                    edge.key to true,
                    DataEdgeKey(edge.key.dst, edge.key.src, edge.key.edge) to false
            ).forEach { (directedEdge, vertexIsSrc) ->

                val vertexEntityKeyId = directedEdge.src.entityKeyId
                val neighborDetails = getNeighborEntityDetails(
                        directedEdge.edge,
                        directedEdge.dst,
                        vertexIsSrc,
                        entitySetsById,
                        entities
                )

                if (entityKeyIds.contains(directedEdge.src.entityKeyId) && neighborDetails != null) {

                    if (!entityNeighbors.containsKey(vertexEntityKeyId)) {
                        entityNeighbors[vertexEntityKeyId] = mutableListOf()
                    }

                    entityNeighbors.getValue(vertexEntityKeyId).add(neighborDetails)
                }

            }

        }

        /* Map linkingIds to the collection of neighbors for all entityKeyIds in the cluster */
        entityKeyIdsByLinkingId.forEach { (linkingId, normalEntityKeyIds) ->
            entityNeighbors[linkingId] = normalEntityKeyIds
                    .flatMap { entityKeyId ->
                        entityNeighbors.getOrDefault(entityKeyId, arrayListOf())
                    }.toMutableList()

        }

        return NeighborPage(entityNeighbors, edges.lastOrNull()?.key)
    }

    private fun getNeighborEntityDetails(
            associationEDK: EntityDataKey,
            neighborEDK: EntityDataKey,
            vertexIsSrc: Boolean,
            entitySetsById: Map<UUID, EntitySet>,
            entities: Map<UUID, Map<FullQualifiedName, Set<Any>>>
    ): NeighborEntityDetails? {

        val edgeDetails = entities[associationEDK.entityKeyId]
        val neighborDetails = entities[neighborEDK.entityKeyId]
        val neighborEntitySet = entitySetsById[neighborEDK.entitySetId]

        if (edgeDetails == null || neighborDetails == null || neighborEntitySet == null) {
            return null
        }

        return NeighborEntityDetails(
                entitySetsById[associationEDK.entitySetId],
                edgeDetails,
                neighborEntitySet,
                neighborEDK.entityKeyId,
                neighborDetails,
                vertexIsSrc
        )

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
    fun executeEntityNeighborIdsSearch(
            entitySetIds: Set<UUID>,
            requestedFilter: EntityNeighborsFilter,
            principals: Set<Principal>
    ): Map<UUID, Map<UUID, SetMultimap<UUID, NeighborEntityIds>>> {

        val (filter, _) = getAuthorizedFilterEntitySetOptions(
                entitySetIds,
                requestedFilter,
                principals
        )

        if (filter.associationEntitySetIds.isPresent && filter.associationEntitySetIds.get().isEmpty()) {
            logger.info("Missing association entity set ids. Returning empty result.")
            return ImmutableMap.of()
        }

        val entityKeyIds = filter.entityKeyIds

        val neighbors = mutableMapOf<UUID, MutableMap<UUID, SetMultimap<UUID, NeighborEntityIds>>>()

        graphService.getEdgesAndNeighborsForVertices(entitySetIds, PagedNeighborRequest(filter)).forEach { edge ->

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

        }

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
                    .associateWith { Optional.of(entityKeyIds) }
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

    fun triggerAllOrganizationsIndex(allOrganizations: List<Organization>) {
        elasticsearchApi.triggerOrganizationIndex(allOrganizations)
    }

    fun triggerOrganizationIndex(organization: Organization) {
        elasticsearchApi.triggerOrganizationIndex(Lists.newArrayList(organization))
    }

}

