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

package com.openlattice.data.controllers

import com.auth0.spring.security.api.authentication.PreAuthenticatedAuthenticationJsonWebToken
import com.codahale.metrics.annotation.Timed
import com.google.common.base.Preconditions
import com.google.common.base.Preconditions.checkState
import com.google.common.cache.LoadingCache
import com.google.common.collect.*
import com.openlattice.authorization.*
import com.openlattice.authorization.EdmAuthorizationHelper.*
import com.openlattice.data.*
import com.openlattice.data.requests.EntitySetSelection
import com.openlattice.data.requests.FileType
import com.openlattice.datastore.data.controllers.AuthorizationKey
import com.openlattice.datastore.data.controllers.DataController
import com.openlattice.datastore.services.EdmService
import com.openlattice.datastore.services.SyncTicketService
import com.openlattice.edm.type.PropertyType
import com.openlattice.web.mediatypes.CustomMediaType
import org.apache.commons.lang3.StringUtils
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.security.authentication.AuthenticationManager
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.web.bind.annotation.*
import java.nio.ByteBuffer
import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream
import javax.inject.Inject
import javax.servlet.http.HttpServletResponse
import kotlin.streams.asSequence

private val logger = LoggerFactory.getLogger(DataController::class.java)
/**
 *
 * Data controller is written in Kotlin
 */
class DataController2 : DataApi, AuthorizingComponent {
    @Inject
    private lateinit var sts: SyncTicketService

    @Inject
    private lateinit var edmService: EdmService

    @Inject
    private lateinit var dgm: DataGraphManager

    @Inject
    private lateinit var authz: AuthorizationManager

    @Inject
    private lateinit var authzHelper: EdmAuthorizationHelper

    @Inject
    private lateinit var authProvider: AuthenticationManager = null

    private val primitiveTypeKinds: LoadingCache<UUID, EdmPrimitiveTypeKind>? = null
    private val authorizedPropertyCache: LoadingCache<AuthorizationKey, Set<UUID>>? = null

    @RequestMapping(
            path = arrayOf("/" + DataApi.ENTITY_SET + "/" + DataApi.SET_ID_PATH), method = arrayOf(
            RequestMethod.GET
    ), produces = arrayOf(MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE)
    )
    fun loadEntitySetData(
            @PathVariable(DataApi.ENTITY_SET_ID) entitySetId: UUID,
            @RequestParam(value = DataApi.FILE_TYPE, required = false)
            fileType: FileType,
            @RequestParam(value = DataApi.TOKEN, required = false)
            token: String,
            response: HttpServletResponse
    ): EntitySetData<FullQualifiedName> {
        setContentDisposition(response, entitySetId.toString(), fileType)
        setDownloadContentType(response, fileType)

        return loadEntitySetData(entitySetId, fileType, token)
    }

    override fun loadEntitySetData(
            entitySetId: UUID,
            fileType: FileType,
            token: String
    ): EntitySetData<FullQualifiedName> {
        if (StringUtils.isNotBlank(token)) {
            val authentication = authProvider!!
                    .authenticate(PreAuthenticatedAuthenticationJsonWebToken.usingToken(token))
            SecurityContextHolder.getContext().authentication = authentication
        }
        return loadEntitySetData(entitySetId, EntitySetSelection(Optional.empty()))
    }

    @RequestMapping(
            path = arrayOf("/" + DataApi.ENTITY_SET + "/" + DataApi.SET_ID_PATH), method = arrayOf(
            RequestMethod.POST
    ), consumes = arrayOf(MediaType.APPLICATION_JSON_VALUE), produces = arrayOf(
            MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE
    )
    )
    fun loadEntitySetData(
            @PathVariable(DataApi.ENTITY_SET_ID) entitySetId: UUID,
            @RequestBody(required = false) selection: EntitySetSelection,
            @RequestParam(value = DataApi.FILE_TYPE, required = false) fileType: FileType,
            response: HttpServletResponse
    ): EntitySetData<FullQualifiedName> {
        setContentDisposition(response, entitySetId.toString(), fileType)
        setDownloadContentType(response, fileType)
        return loadEntitySetData(entitySetId, selection, fileType)
    }

    override fun loadEntitySetData(
            entitySetId: UUID,
            selection: EntitySetSelection,
            fileType: FileType
    ): EntitySetData<FullQualifiedName> {
        return loadEntitySetData(entitySetId, selection)
    }

    private fun loadEntitySetData(
            entitySetId: UUID,
            selection: EntitySetSelection
    ): EntitySetData<FullQualifiedName> {
        if (authz!!.checkIfHasPermissions(
                        AclKey(entitySetId),
                        Principals.getCurrentPrincipals(),
                        EnumSet.of(Permission.READ)
                )) {
            val es = edmService!!.getEntitySet(entitySetId)
            if (es.isLinking) {
                val allEntitySetIds = Sets.newHashSet(es.linkedEntitySets)
                checkState(
                        !allEntitySetIds.isEmpty(),
                        "Linked entity sets are empty for linking entity set %s", entitySetId
                )
                return loadEntitySetData(
                        allEntitySetIds.map { it to selection.entityKeyIds }.toMap(),
                        allEntitySetIds,
                        selection,
                        true
                )
            } else {
                return loadEntitySetData(
                        mapOf(entitySetId to selection.entityKeyIds),
                        setOf(entitySetId),
                        selection,
                        false
                )
            }
        } else {
            throw ForbiddenException("Insufficient permissions to read the entity set or it doesn't exists.")
        }
    }

    private fun loadEntitySetData(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            dataEntitySetIds: Set<UUID>,
            selection: EntitySetSelection,
            linking: Boolean
    ): EntitySetData<FullQualifiedName> {
        val allProperties = authzHelper!!.getAllPropertiesOnEntitySet(dataEntitySetIds.iterator().next())
        val selectedProperties = selection.properties.orElse(allProperties)

        checkState(
                allProperties == selectedProperties || allProperties.containsAll(selectedProperties),
                "Selected properties are not property types of entity set %s",
                dataEntitySetIds.iterator().next()
        )

        val authorizedPropertyTypes = authzHelper.getAuthorizedPropertyTypes(
                dataEntitySetIds,
                selectedProperties,
                EnumSet.of(Permission.READ)
        )


        val allAuthorizedPropertyTypes = authorizedPropertyTypes.values
                .flatMap { it.values }
                .distinct()
                .map { it.id to it }
                .toMap()


        val orderedPropertyNames = LinkedHashSet<String>(allAuthorizedPropertyTypes.size)

        selectedProperties.stream()
                .filter(allAuthorizedPropertyTypes::containsKey)
                .map { allAuthorizedPropertyTypes[it]!! }
                .map { pt -> pt.type.fullQualifiedNameAsString }
                .forEach { orderedPropertyNames.add(it) }

        return dgm!!.getEntitySetData(entityKeyIds, orderedPropertyNames, authorizedPropertyTypes, linking!!)
    }

    @PutMapping(
            value = ["/" + DataApi.ENTITY_SET + "/" + DataApi.SET_ID_PATH],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateEntitiesInEntitySet(
            @PathVariable(DataApi.ENTITY_SET_ID) entitySetId: UUID,
            @RequestBody entities: Map<UUID, Map<UUID, Set<Any>>>,
            @RequestParam(value = DataApi.TYPE, defaultValue = "Merge") updateType: UpdateType
    ): Int {
        Preconditions.checkNotNull(updateType, "An invalid update type value was specified.")
        ensureReadAccess(AclKey(entitySetId))
        val allAuthorizedPropertyTypes = authzHelper!!
                .getAuthorizedPropertyTypes(entitySetId, EnumSet.of(Permission.WRITE))
        val requiredPropertyTypes = requiredEntitySetPropertyTypes(entities)

        accessCheck(allAuthorizedPropertyTypes, requiredPropertyTypes)

        val authorizedPropertyTypes =
                requiredPropertyTypes.map { it to allAuthorizedPropertyTypes[it]!! }.toMap()

        return when (updateType) {
            UpdateType.Replace -> dgm!!.replaceEntities(entitySetId, entities, authorizedPropertyTypes)
            UpdateType.PartialReplace -> dgm!!.partialReplaceEntities(
                    entitySetId, entities, authorizedPropertyTypes
            )
            UpdateType.Merge -> dgm!!.mergeEntities(entitySetId, entities, authorizedPropertyTypes)
            else -> 0
        }
    }

    @PatchMapping(
            value = ["/" + DataApi.ENTITY_SET + "/" + DataApi.SET_ID_PATH],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun replaceEntityProperties(
            @PathVariable(DataApi.ENTITY_SET_ID) entitySetId: UUID,
            @RequestBody entities: Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Any>>>
    ): Int {
        ensureReadAccess(AclKey(entitySetId))

        val requiredPropertyTypes = requiredReplacementPropertyTypes(entities)
        accessCheck(
                aclKeysForAccessCheck(
                        ImmutableSetMultimap.builder<UUID, UUID>()
                                .putAll(entitySetId, requiredPropertyTypes).build(),
                        WRITE_PERMISSION
                )
        )

        return dgm!!.replacePropertiesInEntities(
                entitySetId,
                entities,
                edmService!!.getPropertyTypesAsMap(requiredPropertyTypes)
        )
    }

    @PutMapping(value = ["/" + DataApi.ASSOCIATION], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun createAssociations(@RequestBody associations: Set<DataEdgeKey>): Int {
        //TODO: This allows creating an edge even if you don't have access to key properties on association
        //entity set. Consider requiring access to key properties on association in order to allow creating edge.
        associations.stream()
                .flatMap { dataEdgeKey ->
                    Stream.of(
                            dataEdgeKey.src.entitySetId,
                            dataEdgeKey.dst.entitySetId,
                            dataEdgeKey.edge.entitySetId
                    )
                }

        val entitySetIds = associations
                .flatMap { edgeKey ->
                    listOf(
                            edgeKey.src.entitySetId,
                            edgeKey.dst.entitySetId,
                            edgeKey.edge.entitySetId
                    )
                }
                .toSet()

        //Ensure that we have read access to entity set metadata.
        entitySetIds.forEach { entitySetId -> ensureReadAccess(AclKey(entitySetId)) }

        return dgm!!.createAssociations(associations)
    }

    @Timed
    @RequestMapping(
            value = ["/" + DataApi.ENTITY_SET + "/"]
            , method = [RequestMethod.POST],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun createEntities(
            @RequestParam(DataApi.ENTITY_SET_ID) entitySetId: UUID,
            @RequestBody entities: List<Map<UUID, Set<Any>>>
    ): List<UUID> {
        //Ensure that we have read access to entity set metadata.
        ensureReadAccess(AclKey(entitySetId))
        //Load authorized property types
        val authorizedPropertyTypes = authzHelper!!
                .getAuthorizedPropertyTypes(entitySetId, WRITE_PERMISSION)
        return dgm!!.createEntities(entitySetId, entities, authorizedPropertyTypes)
    }

    @PutMapping(
            value = ["/" + DataApi.ENTITY_SET + "/" + DataApi.SET_ID_PATH + "/" + DataApi.ENTITY_KEY_ID_PATH],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun mergeIntoEntityInEntitySet(
            @PathVariable(DataApi.ENTITY_SET_ID) entitySetId: UUID,
            @PathVariable(DataApi.ENTITY_KEY_ID) entityKeyId: UUID,
            @RequestBody entity: Map<UUID, Set<Any>>
    ): Int {
        val entities = ImmutableMap.of(entityKeyId, entity)
        return updateEntitiesInEntitySet(entitySetId, entities, UpdateType.Merge)
    }

    override fun getAuthorizationManager(): AuthorizationManager? {
        return authz
    }

    @Timed
    @RequestMapping(
            path = arrayOf("/" + DataApi.ASSOCIATION), method = arrayOf(RequestMethod.POST), consumes = arrayOf(
            MediaType.APPLICATION_JSON_VALUE
    )
    )
    override fun createAssociations(@RequestBody associations: ListMultimap<UUID, DataEdge>): ListMultimap<UUID, UUID> {
        //Ensure that we have read access to entity set metadata.
        associations.keySet().forEach { entitySetId -> ensureReadAccess(AclKey(entitySetId)) }

        //Ensure that we can write properties.
        val requiredPropertyTypes = requiredAssociationPropertyTypes(associations)
        accessCheck(aclKeysForAccessCheck(requiredPropertyTypes, WRITE_PERMISSION))

        val authorizedPropertyTypesByEntitySet = associations.keySet()
                .map { entitySetId ->
                    entitySetId to authzHelper.getAuthorizedPropertyTypes(entitySetId, EnumSet.of(Permission.WRITE))
                }.toMap()

        return dgm!!.createAssociations(associations, authorizedPropertyTypesByEntitySet)
    }

    @Timed
    @PatchMapping(value = ["/" + DataApi.ASSOCIATION])
    override fun replaceAssociationData(
            @RequestBody associations: Map<UUID, Map<UUID, DataEdge>>,
            @RequestParam(value = DataApi.PARTIAL, required = false, defaultValue = "false") partial: Boolean
    ): Int {
        associations.keys.forEach { entitySetId -> ensureReadAccess(AclKey(entitySetId)) }

        //Ensure that we can write properties.
        val requiredPropertyTypes = requiredAssociationPropertyTypes(associations)
        accessCheck(aclKeysForAccessCheck(requiredPropertyTypes, WRITE_PERMISSION))

        val authorizedPropertyTypes = edmService!!
                .getPropertyTypesAsMap(ImmutableSet.copyOf(requiredPropertyTypes.values()))

        return associations.entries.map { association ->
            val entitySetId = association.key
            if (partial) {
                return@map dgm.partialReplaceEntities(
                        entitySetId,
                        association.value.mapValues { it.value.data },
                        authorizedPropertyTypes
                )
            } else {
                return@map dgm.replaceEntities(
                        entitySetId,
                        association.value.mapValues { it.value.data },
                        authorizedPropertyTypes
                )
            }
        }.sum()
    }

    @Timed
    @PostMapping(value = ["/", ""])
    override fun createEntityAndAssociationData(@RequestBody data: DataGraph): DataGraphIds {
        val entityKeyIds = ArrayListMultimap.create<UUID, UUID>()
        val associationEntityKeyIds: ListMultimap<UUID, UUID>

        //First create the entities so we have entity key ids to work with
        Multimaps.asMap(data.entities)
                .forEach { entitySetId, entities ->
                    entityKeyIds.putAll(
                            entitySetId, createEntities(entitySetId, entities)
                    )
                }
        val toBeCreated = ArrayListMultimap.create<UUID, DataEdge>()
        Multimaps.asMap(data.associations)
                .forEach { entitySetId, associations ->
                    for (association in associations) {
                        val srcEntitySetId = association.srcEntitySetId
                        val srcEntityKeyId = association
                                .srcEntityKeyId
                                .orElseGet { entityKeyIds.get(srcEntitySetId)[association.srcEntityIndex.get()] }

                        val dstEntitySetId = association.dstEntitySetId
                        val dstEntityKeyId = association
                                .dstEntityKeyId
                                .orElseGet { entityKeyIds.get(dstEntitySetId)[association.dstEntityIndex.get()] }

                        toBeCreated.put(
                                entitySetId,
                                DataEdge(
                                        EntityDataKey(srcEntitySetId, srcEntityKeyId),
                                        EntityDataKey(dstEntitySetId, dstEntityKeyId),
                                        association.data
                                )
                        )
                    }
                }
        associationEntityKeyIds = createAssociations(toBeCreated)

        return DataGraphIds(entityKeyIds, associationEntityKeyIds)
    }

    @RequestMapping(
            path = ["/" + DataApi.ENTITY_SET + "/" + DataApi.SET_ID_PATH + "/" + DataApi.ENTITY_KEY_ID_PATH],
            method = [RequestMethod.DELETE]
    )
    override fun clearEntityFromEntitySet(
            @PathVariable(DataApi.ENTITY_SET_ID) entitySetId: UUID,
            @PathVariable(DataApi.ENTITY_KEY_ID) entityKeyId: UUID
    ): Void? {
        ensureReadAccess(AclKey(entitySetId))
        //Note this will only clear properties to which the caller has access.
        dgm!!.clearEntities(
                entitySetId,
                ImmutableSet.of(entityKeyId),
                authzHelper!!.getAuthorizedPropertyTypes(entitySetId, WRITE_PERMISSION)
        )
        return null
    }

    @RequestMapping(
            path = ["/" + DataApi.ENTITY_SET + "/" + DataApi.SET_ID_PATH + "/" + DataApi.ENTITIES],
            method = [RequestMethod.DELETE]
    )
    override fun clearAllEntitiesFromEntitySet(@PathVariable(DataApi.ENTITY_SET_ID) entitySetId: UUID): Int? {
        ensureOwnerAccess(AclKey(entitySetId))
        val entitySet = edmService!!.getEntitySet(entitySetId)
        if (entitySet.isLinking) {
            throw ForbiddenException("You cannot clear all data from a linked entity set.")
        }

        val entityType = edmService.getEntityType(entitySet.entityTypeId)
        val authorizedPropertyTypes = authzHelper!!
                .getAuthorizedPropertyTypes(entitySetId, EnumSet.of(Permission.OWNER))
        if (!authorizedPropertyTypes.keys.containsAll(entityType.properties)) {
            throw ForbiddenException(
                    "You must be an owner of all entity set properties to clear the entity set data."
            )
        }

        return dgm!!.clearEntitySet(entitySetId, authorizedPropertyTypes)
    }

    override fun deleteEntityProperties(
            entitySetId: UUID, entityProperties: Map<UUID, Map<UUID, Set<ByteBuffer>>>
    ): Int? {
        return null
    }

    @RequestMapping(
            path = ["/" + DataApi.ENTITY_SET + "/" + DataApi.SET_ID_PATH],
            method = [RequestMethod.DELETE]
    )
    override fun clearEntitySet(@PathVariable(DataApi.ENTITY_SET_ID) entitySetId: UUID): Int? {
        ensureOwnerAccess(AclKey(entitySetId))
        return dgm!!
                .clearEntitySet(entitySetId, authzHelper!!.getAuthorizedPropertyTypes(entitySetId, WRITE_PERMISSION))
    }

    @Timed
    @RequestMapping(
            path = ["/" + DataApi.ENTITY_SET + "/" + DataApi.SET_ID_PATH + "/" + DataApi.ENTITY_KEY_ID_PATH + "/" + DataApi.NEIGHBORS],
            method = [RequestMethod.DELETE]
    )
    override fun clearEntityAndNeighborEntities(
            @PathVariable(DataApi.ENTITY_SET_ID) vertexEntitySetId: UUID,
            @PathVariable(DataApi.ENTITY_KEY_ID) vertexEntityKeyId: UUID
    ): Long? {

        /*
         * 1 - collect all relevant EntitySets and PropertyTypes
         */

        // TODO: linking entity sets , linking id (if needed)
        // getNeighborEntitySetIds() returns source, destination, and edge EntitySet ids
        val neighborEntitySetIds = dgm!!.getNeighborEntitySetIds(ImmutableSet.of(vertexEntitySetId))
        val allEntitySetIds = ImmutableSet.builder<UUID>()
                .add(vertexEntitySetId)
                .addAll(neighborEntitySetIds)
                .build()

        val entitySetsAccessRequestMap = allEntitySetIds
                .map { AclKey(it) to READ_PERMISSION }
                .toMap()

        val entitySetIdToPropertyTypesMap = Maps.newHashMap<UUID, Map<UUID, PropertyType>>()
        val propertyTypesAccessRequestMap = allEntitySetIds

                .flatMap { esId ->
                    val propertyTypes = edmService.getPropertyTypesForEntitySet(esId)
                    entitySetIdToPropertyTypesMap[esId] = propertyTypes
                    entitySetIdToPropertyTypesMap.keys.map { ptId -> AclKey(esId, ptId) to WRITE_PERMISSION }
                }.toMap()

        /*
         * 2 - check permissions for all relevant EntitySets and PropertyTypes
         */

        val accessRequestMap = Maps.newHashMap<AclKey, EnumSet<Permission>>()
        accessRequestMap.putAll(entitySetsAccessRequestMap)
        accessRequestMap.putAll(propertyTypesAccessRequestMap)

        accessCheck(accessRequestMap)

        /*
         * 3 - collect all neighbor entities, organized by EntitySet
         */

        val entitySetIdToEntityDataKeysMap = dgm
                .getEdgesAndNeighborsForVertex(vertexEntitySetId, vertexEntityKeyId)
                .flatMap { edge -> Stream.of(edge.src, edge.dst, edge.edge) }
                .collect(                        Collectors.groupingBy{(edk:EntityDataKey-> it.}
                                Function<EntityDataKey, UUID>{ it.getEntitySetId() }, Collectors.toSet()
                        )
                )

        /*
         * 4 - clear all entities
         */

        if (allEntitySetIds.containsAll(entitySetIdToEntityDataKeysMap.keys)) {
            entitySetIdToEntityDataKeysMap.forEach { entitySetId, entityDataKeys ->
                dgm.clearEntities(
                        entitySetId,
                        entityDataKeys.stream().map<UUID>(
                                Function<EntityDataKey, UUID> { it.getEntityKeyId() }).collect<Set<UUID>, Any>(
                                Collectors.toSet()
                        ),
                        entitySetIdToPropertyTypesMap[entitySetId]
                )
            }
            return entitySetIdToEntityDataKeysMap
                    .entries
                    .stream()
                    .mapToLong { entry -> entry.value.size }
                    .sum()
        } else {
            throw ForbiddenException("Insufficient permissions to perform operation.")
        }
    }

    @RequestMapping(
            path = arrayOf("/" + DataApi.ENTITY_SET + "/" + DataApi.SET_ID_PATH + "/" + DataApi.ENTITY_KEY_ID_PATH),
            method = arrayOf(
                    RequestMethod.PUT
            )
    )
    override fun replaceEntityInEntitySet(
            @PathVariable(DataApi.ENTITY_SET_ID) entitySetId: UUID,
            @PathVariable(DataApi.ENTITY_KEY_ID) entityKeyId: UUID,
            @RequestBody entity: Map<UUID, Set<Any>>
    ): Int? {
        ensureReadAccess(AclKey(entitySetId))
        val authorizedPropertyTypes = authzHelper!!.getAuthorizedPropertyTypes(
                entitySetId,
                WRITE_PERMISSION,
                edmService!!.getPropertyTypesAsMap(entity.keys)
        )

        return dgm!!.replaceEntities(entitySetId, ImmutableMap.of(entityKeyId, entity), authorizedPropertyTypes)
    }

    @RequestMapping(
            path = arrayOf("/" + DataApi.ENTITY_SET + "/" + DataApi.SET_ID_PATH + "/" + DataApi.ENTITY_KEY_ID_PATH),
            method = arrayOf(
                    RequestMethod.POST
            )
    )
    override fun replaceEntityInEntitySetUsingFqns(
            @PathVariable(DataApi.ENTITY_SET_ID) entitySetId: UUID,
            @PathVariable(DataApi.ENTITY_KEY_ID) entityKeyId: UUID,
            @RequestBody entityByFqns: Map<FullQualifiedName, Set<Any>>
    ): Int? {
        val entity = HashMap<UUID, Set<Any>>()

        entityByFqns
                .forEach { fqn, properties -> entity[edmService!!.getPropertyTypeId(fqn)] = properties }

        return replaceEntityInEntitySet(entitySetId, entityKeyId, entity)
    }

    @RequestMapping(
            path = arrayOf("/" + DataApi.SET_ID_PATH + "/" + DataApi.COUNT), method = arrayOf(RequestMethod.GET)
    )
    override fun getEntitySetSize(@PathVariable(DataApi.ENTITY_SET_ID) entitySetId: UUID): Long {
        ensureReadAccess(AclKey(entitySetId))

        val es = edmService!!.getEntitySet(entitySetId)
        // If entityset is linking: should return distinct count of entities corresponding to the linking entity set,
        // which is the distinct count of linking_id s
        return if (es.isLinking) {
            dgm!!.getLinkingEntitySetSize(es.linkedEntitySets)
        } else {
            dgm!!.getEntitySetSize(entitySetId)
        }
    }

    @RequestMapping(
            path = arrayOf("/" + DataApi.SET_ID_PATH + "/" + DataApi.ENTITY_KEY_ID_PATH), method = arrayOf(
            RequestMethod.GET
    )
    )
    override fun getEntity(
            @PathVariable(DataApi.ENTITY_SET_ID) entitySetId: UUID,
            @PathVariable(DataApi.ENTITY_KEY_ID) entityKeyId: UUID
    ): SetMultimap<FullQualifiedName, Any> {
        ensureReadAccess(AclKey(entitySetId))
        val es = edmService!!.getEntitySet(entitySetId)

        if (es.isLinking) {
            val allProperties = authzHelper!!.getAllPropertiesOnEntitySet(
                    es.linkedEntitySets.iterator().next()
            )
            checkState(
                    !es.linkedEntitySets.isEmpty(),
                    "Linked entity sets are empty for linking entity set %s", entitySetId
            )

            val authorizedPropertyTypes = authzHelper
                    .getAuthorizedPropertyTypes(
                            es.linkedEntitySets,
                            allProperties,
                            EnumSet.of(Permission.READ)
                    )

            return dgm!!.getLinkingEntity(es.linkedEntitySets, entityKeyId, authorizedPropertyTypes)
        } else {
            val authorizedPropertyTypes = edmService.getPropertyTypesAsMap(
                    authzHelper!!.getAuthorizedPropertiesOnEntitySet(entitySetId, READ_PERMISSION)
            )
            return dgm!!.getEntity(entitySetId, entityKeyId, authorizedPropertyTypes)
        }
    }

    @GetMapping(
            path = arrayOf(
                    "/" + DataApi.SET_ID_PATH + "/" + DataApi.ENTITY_KEY_ID_PATH + "/" + DataApi.PROPERTY_TYPE_ID_PATH
            ), produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getEntity(entitySetId: UUID, entityKeyId: UUID, propertyTypeId: UUID): Set<Any> {
        ensureReadAccess(AclKey(entitySetId))
        val es = edmService!!.getEntitySet(entitySetId)

        if (es.isLinking) {
            checkState(
                    !es.linkedEntitySets.isEmpty(),
                    "Linked entity sets are empty for linking entity set %s", entitySetId
            )

            val authorizedPropertyTypes = authzHelper!!
                    .getAuthorizedPropertyTypes(
                            es.linkedEntitySets,
                            Set.of<UUID>(propertyTypeId),
                            EnumSet.of(Permission.READ)
                    )
            val propertyTypeFqn = authorizedPropertyTypes[entitySetId][propertyTypeId]
                    .getType()
            return dgm!!.getLinkingEntity(es.linkedEntitySets, entityKeyId, authorizedPropertyTypes)
                    .get(propertyTypeFqn)
        } else {
            ensureReadAccess(AclKey(entitySetId, propertyTypeId))
            val authorizedPropertyTypes = edmService
                    .getPropertyTypesAsMap(ImmutableSet.of(propertyTypeId))
            val propertyTypeFqn = authorizedPropertyTypes[propertyTypeId].getType()

            return dgm!!.getEntity(entitySetId, entitySetId, authorizedPropertyTypes)
                    .get(propertyTypeFqn)
        }
    }

    /**
     * Methods for setting http response header
     */

    private fun setDownloadContentType(response: HttpServletResponse, fileType: FileType) {
        if (fileType == FileType.csv) {
            response.contentType = CustomMediaType.TEXT_CSV_VALUE
        } else {
            response.contentType = MediaType.APPLICATION_JSON_VALUE
        }
    }

    private fun setContentDisposition(
            response: HttpServletResponse,
            fileName: String,
            fileType: FileType
    ) {
        if (fileType == FileType.csv || fileType == FileType.json) {
            response.setHeader(
                    "Content-Disposition",
                    "attachment; filename=" + fileName + "." + fileType.toString()
            )
        }
    }

    private fun requiredAssociationPropertyTypes(associations: ListMultimap<UUID, DataEdge>): SetMultimap<UUID, UUID> {
        val propertyTypesByEntitySet = HashMultimap.create<UUID, UUID>()
        associations.entries().forEach { entry ->
            propertyTypesByEntitySet
                    .putAll(entry.key, entry.value.data.keys)
        }
        return propertyTypesByEntitySet
    }

    private fun requiredAssociationPropertyTypes(
            associations: Map<UUID, Map<UUID, DataEdge>>
    ): SetMultimap<UUID, UUID> {
        val propertyTypesByEntitySet = HashMultimap.create<UUID, UUID>()
        associations.forEach { esId, edges ->
            edges.values
                    .forEach { de -> propertyTypesByEntitySet.putAll(esId, de.data.keys) }
        }
        return propertyTypesByEntitySet
    }

    private fun requiredEntitySetPropertyTypes(entities: Map<UUID, Map<UUID, Set<Any>>>): Set<UUID> {
        return entities.values.stream().map<Set<UUID>>(
                Function<Map<UUID, Set<Any>>, Set<UUID>> { it.keys }).flatMap<UUID>(
                Function<Set<UUID>, Stream<out UUID>> { it.stream() })
                .collect<Set<UUID>, Any>(Collectors.toSet())
    }

    private fun requiredReplacementPropertyTypes(
            entities: Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Any>>>
    ): Set<UUID> {
        return entities.values.stream().map<Set<UUID>>(
                Function<SetMultimap<UUID, Map<ByteBuffer, Any>>, Set<UUID>> { it.keySet() }).flatMap<UUID>(
                Function<Set<UUID>, Stream<out UUID>> { it.stream() })
                .collect<Set<UUID>, Any>(Collectors.toSet())
    }

    private fun requiredPropertyAuthorizations(entities: Collection<SetMultimap<UUID, Any>>): Set<UUID> {
        return entities.stream().map<Set<UUID>>(
                Function<SetMultimap<UUID, Any>, Set<UUID>> { it.keySet() }).flatMap<UUID>(
                Function<Set<UUID>, Stream<out UUID>> { it.stream() }).collect<Set<UUID>, Any>(
                Collectors.toSet()
        )
    }
}