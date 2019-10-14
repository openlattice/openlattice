/*
 * Copyright (C) 2018. OpenLattice, Inc.
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
 */

package com.openlattice.entitysets.controllers

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.openlattice.auditing.AuditEventType
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.auditing.AuditableEvent
import com.openlattice.auditing.AuditingComponent
import com.openlattice.IdConstants
import com.openlattice.auditing.*
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.authorization.util.AuthorizationUtils
import com.openlattice.controllers.exceptions.ForbiddenException
import com.openlattice.controllers.exceptions.wrappers.BatchException
import com.openlattice.controllers.exceptions.wrappers.ErrorsDTO
import com.openlattice.controllers.util.ApiExceptions
import com.openlattice.data.DataExpiration
import com.openlattice.data.DataGraphManager
import com.openlattice.data.WriteEvent
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.set.EntitySetPropertyMetadata
import com.openlattice.edm.set.ExpirationBase
import com.openlattice.edm.type.PropertyType
import com.openlattice.entitysets.EntitySetsApi
import com.openlattice.entitysets.EntitySetsApi.Companion.ALL
import com.openlattice.entitysets.EntitySetsApi.Companion.ID
import com.openlattice.entitysets.EntitySetsApi.Companion.IDS_PATH
import com.openlattice.entitysets.EntitySetsApi.Companion.ID_PATH
import com.openlattice.entitysets.EntitySetsApi.Companion.EXPIRATION_PATH
import com.openlattice.entitysets.EntitySetsApi.Companion.LINKING
import com.openlattice.entitysets.EntitySetsApi.Companion.METADATA_PATH
import com.openlattice.entitysets.EntitySetsApi.Companion.NAME
import com.openlattice.entitysets.EntitySetsApi.Companion.NAME_PATH
import com.openlattice.entitysets.EntitySetsApi.Companion.PROPERTIES_PATH
import com.openlattice.entitysets.EntitySetsApi.Companion.PROPERTY_TYPE_ID
import com.openlattice.entitysets.EntitySetsApi.Companion.PROPERTY_TYPE_ID_PATH
import com.openlattice.linking.util.PersonProperties
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.VERSIONS
import com.openlattice.postgres.PostgresDataTables
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import retrofit2.http.PUT
import retrofit2.http.Path
import java.sql.Types
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*
import javax.inject.Inject
import kotlin.streams.asSequence

@SuppressFBWarnings(
        value = ["BC_BAD_CAST_TO_ABSTRACT_COLLECTION"],
        justification = "Allowing kotlin collection mapping cast to List")
@RestController
@RequestMapping(EntitySetsApi.CONTROLLER)
class EntitySetsController @Inject
constructor(
        private val authorizations: AuthorizationManager,
        private val edmManager: EdmManager,
        private val aresManager: AuditRecordEntitySetsManager,
        private val auditingManager: AuditingManager,
        private val dgm: DataGraphManager,
        private val spm: SecurePrincipalsManager,
        private val authzHelper: EdmAuthorizationHelper,
        private val securableObjectTypes: SecurableObjectResolveTypeService
) : EntitySetsApi, AuthorizingComponent, AuditingComponent {

    override fun getAuditingManager(): AuditingManager {
        return auditingManager
    }

    private val internalIds: Set<UUID> = IdConstants.values().map { it.id }.toSet() + edmManager.getEntitySet(EDM_AUDIT_ENTITY_SET_NAME).id


    @Timed
    @RequestMapping(path = [LINKING + ID_PATH], method = [RequestMethod.PUT])
    override fun addEntitySetsToLinkingEntitySet(
            @PathVariable(ID) linkingEntitySetId: UUID,
            @RequestBody entitySetIds: Set<UUID>
    ): Int {
        return addEntitySets(linkingEntitySetId, entitySetIds)
    }

    @Timed
    @RequestMapping(path = [LINKING], method = [RequestMethod.POST])
    override fun addEntitySetsToLinkingEntitySets(@RequestBody entitySetIds: Map<UUID, Set<UUID>>): Int {
        return entitySetIds.map { addEntitySets(it.key, it.value) }.sum()
    }

    private fun addEntitySets(linkingEntitySetId: UUID, entitySetIds: Set<UUID>): Int {
        ensureOwnerAccess(AclKey(linkingEntitySetId))
        Preconditions.checkState(
                edmManager.getEntitySet(linkingEntitySetId).isLinking,
                "Can't add linked entity sets to a not linking entity set"
        )
        checkLinkedEntitySets(entitySetIds)
        ensureValidLinkedEntitySets(entitySetIds)

        return edmManager.addLinkedEntitySets(linkingEntitySetId, entitySetIds)
    }

    @Timed
    @RequestMapping(path = [LINKING + ID_PATH], method = [RequestMethod.DELETE])
    override fun removeEntitySetsFromLinkingEntitySet(
            @PathVariable(ID) linkingEntitySetId: UUID,
            @RequestBody entitySetIds: Set<UUID>
    ): Int {
        return removeEntitySets(linkingEntitySetId, entitySetIds)
    }

    @Timed
    @RequestMapping(path = [LINKING], method = [RequestMethod.DELETE])
    override fun removeEntitySetsFromLinkingEntitySets(@RequestBody entitySetIds: Map<UUID, Set<UUID>>): Int {
        return entitySetIds.map { removeEntitySets(it.key, it.value) }.sum()
    }

    @Timed
    @RequestMapping(
            path = ["", "/"], method = [RequestMethod.POST],
            consumes = [MediaType.APPLICATION_JSON_VALUE], produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun createEntitySets(@RequestBody entitySets: Set<EntitySet>): Map<String, UUID> {
        val dto = ErrorsDTO()

        val createdEntitySets = Maps.newHashMapWithExpectedSize<String, UUID>(entitySets.size)
        val auditableEvents = Lists.newArrayList<AuditableEvent>()

        // TODO: Add access check to make sure user can create entity sets.
        for (entitySet in entitySets) {
            try {
                ensureValidEntitySet(entitySet)
                edmManager.createEntitySet(Principals.getCurrentUser(), entitySet)
                createdEntitySets[entitySet.name] = entitySet.id

                auditableEvents.add(
                        AuditableEvent(
                                getCurrentUserId(),
                                AclKey(entitySet.id),
                                AuditEventType.CREATE_ENTITY_SET,
                                "Created entity set through EntitySetApi.createEntitySets",
                                Optional.empty(),
                                ImmutableMap.of("entitySet", entitySet),
                                OffsetDateTime.now(),
                                Optional.empty()
                        )
                )
            } catch (e: Exception) {
                dto.addError(ApiExceptions.OTHER_EXCEPTION, entitySet.name + ": " + e.message)
            }

        }

        recordEvents(auditableEvents)

        if (!dto.isEmpty()) {
            throw BatchException(dto)
        }
        return createdEntitySets
    }

    @Timed
    @RequestMapping(path = ["", "/"], method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getEntitySets(): Set<EntitySet> {
        return authorizations.getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                SecurableObjectType.EntitySet,
                EnumSet.of(Permission.READ)
        )
                .asSequence()
                .map { AuthorizationUtils.getLastAclKeySafely(it) }
                .map { edmManager.getEntitySet(it) }
                .toSet()
    }


    @Timed
    @RequestMapping(path = [ALL + ID_PATH], method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getEntitySet(@PathVariable(ID) entitySetId: UUID): EntitySet {
        ensureReadAccess(AclKey(entitySetId))

        recordEvent(
                AuditableEvent(
                        getCurrentUserId(),
                        AclKey(entitySetId),
                        AuditEventType.READ_ENTITY_SET,
                        "Entity set read through EntitySetApi.getEntitySet",
                        Optional.empty(),
                        ImmutableMap.of(),
                        OffsetDateTime.now(),
                        Optional.empty()
                )
        )
        return edmManager.getEntitySet(entitySetId)
    }


    @Timed
    @RequestMapping(path = [ALL + ID_PATH], method = [RequestMethod.DELETE])
    override fun deleteEntitySet(@PathVariable(ID) entitySetId: UUID): Int {
        ensureOwnerAccess(AclKey(entitySetId))
        val deleted: List<WriteEvent>
        val entitySet = edmManager.getEntitySet(entitySetId)

        ensureEntitySetCanBeDeleted(entitySet)

        val entityType = edmManager.getEntityType(entitySet.entityTypeId)
        val authorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes(entitySetId, EnumSet.of(Permission.OWNER))
        if (!authorizedPropertyTypes.keys.containsAll(entityType.properties)) {
            throw ForbiddenException("You shall not pass!")
        }

        // linking entitysets have no entities or associations
        deleted = if (!entitySet.isLinking) {
            // associations need to be deleted first, because edges are deleted in DataGraphManager.deleteEntitySet call
            deleteAssociationsOfEntitySet(entitySetId) + dgm.deleteEntitySet(entitySetId, authorizedPropertyTypes)
        } else listOf(WriteEvent(System.currentTimeMillis(), 1))

        edmManager.deleteEntitySet(entitySetId)
        securableObjectTypes.deleteSecurableObjectType(AclKey(entitySetId))

        deleteAuditEntitySetsForId(entitySetId)

        recordEvent(
                AuditableEvent(
                        getCurrentUserId(),
                        AclKey(entitySetId),
                        AuditEventType.DELETE_ENTITY_SET,
                        "Entity set deleted through EntitySetApi.deleteEntitySet",
                        Optional.empty(),
                        ImmutableMap.of(),
                        OffsetDateTime.now(),
                        Optional.empty()
                )
        )

        return deleted.sumBy { it.numUpdates }
    }

    @Timed
    @RequestMapping(
            path = [IDS_PATH + NAME_PATH],
            method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getEntitySetId(@PathVariable(NAME) entitySetName: String): UUID {
        val es = edmManager.getEntitySet(entitySetName)
        ensureReadAccess(AclKey(es.id))
        Preconditions.checkNotNull<EntitySet>(es, "Entity Set %s does not exists.", entitySetName)
        return es.id
    }

    @Timed
    @RequestMapping(
            path = [IDS_PATH],
            method = [RequestMethod.POST],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getEntitySetIds(@RequestBody entitySetNames: Set<String>): Map<String, UUID> {
        val entitySetIds = edmManager.getAclKeyIds(entitySetNames)
        entitySetIds.values.forEach { entitySetId -> ensureReadAccess(AclKey(entitySetId)) }
        return entitySetIds
    }

    @Timed
    @RequestMapping(
            path = [ALL + ID_PATH + METADATA_PATH],
            method = [RequestMethod.PATCH],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateEntitySetMetadata(
            @PathVariable(ID) entitySetId: UUID,
            @RequestBody update: MetadataUpdate
    ): Int {
        ensureOwnerAccess(AclKey(entitySetId))
        edmManager.updateEntitySetMetadata(entitySetId, update)

        recordEvent(
                AuditableEvent(
                        getCurrentUserId(),
                        AclKey(entitySetId),
                        AuditEventType.UPDATE_ENTITY_SET,
                        "Entity set metadata updated through EntitySetApi.updateEntitySetMetadata",
                        Optional.empty(),
                        ImmutableMap.of("update", update),
                        OffsetDateTime.now(),
                        Optional.empty()
                )
        )

        //TODO: Return number of fields updated? Low priority.
        return 1
    }


    @Timed
    @RequestMapping(
            path = [ALL + METADATA_PATH],
            method = [RequestMethod.POST],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getPropertyMetadataForEntitySets(
            @RequestBody entitySetIds: Set<UUID>
    ): Map<UUID, Map<UUID, EntitySetPropertyMetadata>> {
        val accessChecks = entitySetIds.map { id -> AccessCheck(AclKey(id), EnumSet.of(Permission.READ)) }.toSet()
        authorizations
                .accessChecksForPrincipals(accessChecks, Principals.getCurrentPrincipals())
                .forEach { authorization ->
                    if (!authorization.permissions.getValue(Permission.READ)) {
                        throw ForbiddenException(
                                "AclKey " + authorization.aclKey.toString() + " is not authorized."
                        )
                    }
                }

        return edmManager.getAllEntitySetPropertyMetadataForIds(entitySetIds)
    }

    @Timed
    @RequestMapping(
            path = [ALL + ID_PATH + METADATA_PATH],
            method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getAllEntitySetPropertyMetadata(
            @PathVariable(ID) entitySetId: UUID
    ): Map<UUID, EntitySetPropertyMetadata> {
        //You should be able to get properties without having read access
        ensureReadAccess(AclKey(entitySetId))
        return edmManager.getAllEntitySetPropertyMetadata(entitySetId)
    }

    @Timed
    @RequestMapping(
            path = [ALL + ID_PATH + PROPERTIES_PATH + PROPERTY_TYPE_ID_PATH],
            method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getEntitySetPropertyMetadata(
            @PathVariable(ID) entitySetId: UUID,
            @PathVariable(PROPERTY_TYPE_ID) propertyTypeId: UUID
    ): EntitySetPropertyMetadata {
        ensureReadAccess(AclKey(entitySetId, propertyTypeId))
        return edmManager.getEntitySetPropertyMetadata(entitySetId, propertyTypeId)
    }

    @Timed
    @GetMapping(
            value = [ALL + ID_PATH + PROPERTIES_PATH],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getPropertyTypesForEntitySet(@PathVariable(ID) entitySetId: UUID): Map<UUID, PropertyType> {
        //We only check for entity set metadata read access.
        ensureReadAccess(AclKey(entitySetId))
        return edmManager.getPropertyTypesForEntitySet(entitySetId)
    }

    @Timed
    @RequestMapping(
            path = [ALL + ID_PATH + PROPERTIES_PATH + PROPERTY_TYPE_ID_PATH],
            method = [RequestMethod.POST],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateEntitySetPropertyMetadata(
            @PathVariable(ID) entitySetId: UUID,
            @PathVariable(PROPERTY_TYPE_ID) propertyTypeId: UUID,
            @RequestBody update: MetadataUpdate
    ): Int {
        ensureOwnerAccess(AclKey(entitySetId, propertyTypeId))
        edmManager.updateEntitySetPropertyMetadata(entitySetId, propertyTypeId, update)

        recordEvent(
                AuditableEvent(
                        getCurrentUserId(),
                        AclKey(entitySetId, propertyTypeId),
                        AuditEventType.UPDATE_ENTITY_SET_PROPERTY_METADATA,
                        "Entity set property metadata updated through EntitySetApi.updateEntitySetPropertyMetadata",
                        Optional.empty(),
                        ImmutableMap.of("update", update),
                        OffsetDateTime.now(),
                        Optional.empty()
                )
        )

        //TODO: Makes this return something more useful.
        return 1
    }

    @Timed
    @RequestMapping(
            path = [ALL + ID_PATH + EXPIRATION_PATH],
            method = [RequestMethod.PATCH],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun removeDataExpirationPolicy(
            @PathVariable(ID) entitySetId: UUID
    ): Int {
        ensureOwnerAccess(AclKey(entitySetId))
        edmManager.removeDataExpirationPolicy(entitySetId)

        recordEvent(
                AuditableEvent(
                        getCurrentUserId(),
                        AclKey(entitySetId),
                        AuditEventType.UPDATE_ENTITY_SET,
                        "Entity set data expiration policy removed through EntitySetApi.removeDataExpirationPolicy",
                        Optional.empty(),
                        ImmutableMap.of("entitySetId", entitySetId),
                        OffsetDateTime.now(),
                        Optional.empty()
                )
        )
        return 1
    }

    @Timed
    @RequestMapping(
            path = [ALL + ID_PATH + EXPIRATION_PATH],
            method = [RequestMethod.POST]
    )
    override fun getExpiringEntitiesFromEntitySet(
            @PathVariable(ID) entitySetId: UUID,
            @RequestBody dateTimeAsString: String): Set<UUID> {
        ensureReadAccess(AclKey(entitySetId))
        val dateTime = OffsetDateTime.parse(dateTimeAsString)
        val es = getEntitySet(entitySetId)
        check(es.hasExpirationPolicy()) { "Entity set ${es.name} does not have an expiration policy" }

        val expirationPolicy = es.expiration
        val expirationPT = expirationPolicy.startDateProperty.map { edmManager.getPropertyType(it) }
        return dgm.getExpiringEntitiesFromEntitySet(entitySetId, expirationPolicy, dateTime, es.expiration.deleteType, expirationPT).toSet()
    }



    private fun removeEntitySets(linkingEntitySetId: UUID, entitySetIds: Set<UUID>): Int {
        ensureOwnerAccess(AclKey(linkingEntitySetId))
        Preconditions.checkState(
                edmManager.getEntitySet(linkingEntitySetId).isLinking,
                "Can't remove linked entity sets from a not linking entity set"
        )
        checkLinkedEntitySets(entitySetIds)

        return edmManager.removeLinkedEntitySets(linkingEntitySetId, entitySetIds)
    }

    private fun checkLinkedEntitySets(entitySetIds: Set<UUID>) {
        checkNotNull(entitySetIds)
        Preconditions.checkState(entitySetIds.isNotEmpty(), "Linked entity sets is empty")
    }

    private fun ensureValidLinkedEntitySets(entitySetIds: Set<UUID>) {
        val entityTypeId = edmManager.getEntityType(PersonProperties.PERSON_TYPE_FQN).id
        Preconditions.checkState(
                entitySetIds.stream()
                        .map { edmManager.getEntitySet(it).entityTypeId }
                        .allMatch { entityTypeId == it },
                "Linked entity sets are of differing entity types than %s :{}",
                PersonProperties.PERSON_TYPE_FQN.fullQualifiedNameAsString, entitySetIds
        )

        Preconditions.checkState(
                entitySetIds.all { !edmManager.getEntitySet(it).isLinking },
                "Cannot add linking entity set as linked entity set."
        )
    }

    private fun ensureValidEntitySet(entitySet: EntitySet) {
        Preconditions.checkArgument(
                edmManager.checkEntityTypeExists(entitySet.entityTypeId),
                "Entity Set Type does not exists."
        )

        if (entitySet.isLinking) {
            entitySet.linkedEntitySets.forEach { linkedEntitySetId ->
                Preconditions.checkArgument(
                        edmManager.getEntityTypeByEntitySetId(linkedEntitySetId).id == entitySet.entityTypeId,
                        "Entity type of linked entity sets must be the same as of the linking entity set"
                )
                Preconditions.checkArgument(
                        !edmManager.getEntitySet(linkedEntitySetId).isLinking,
                        "Cannot add linking entity set as linked entity set."
                )
            }
        }
    }

    private fun deleteAssociationsOfEntitySet(entitySetId: UUID): List<WriteEvent> {
        // collect association entity key ids
        val associationsEdgeKeys = dgm.getEdgeKeysOfEntitySet(entitySetId)

        // access checks
        val authorizedPropertyTypes = HashMap<UUID, Map<UUID, PropertyType>>()
        associationsEdgeKeys.stream().forEach { edgeKey ->
            if (!authorizedPropertyTypes.containsKey(edgeKey.edge.entitySetId)) {
                val authorizedPropertyTypesOfAssociation = getAuthorizedPropertyTypesForDelete(
                        edgeKey.edge.entitySetId, Optional.empty()
                )
                authorizedPropertyTypes[edgeKey.edge.entitySetId] = authorizedPropertyTypesOfAssociation
            }
        }

        // delete associations of entity set
        return dgm.deleteAssociationsBatch(entitySetId, associationsEdgeKeys, authorizedPropertyTypes)
    }

    private fun getAuthorizedPropertyTypesForDelete(
            entitySetId: UUID,
            properties: Optional<Set<UUID>>
    ): Map<UUID, PropertyType> {
        ensureOwnerAccess(AclKey(entitySetId))
        val entitySet = getEntitySet(entitySetId)
        if (entitySet.isLinking) {
            throw IllegalArgumentException("You cannot delete entities from a linking entity set.")
        }

        val entityType = edmManager.getEntityType(entitySet.entityTypeId)
        val requiredProperties = properties.orElse(entityType.properties)
        val authorizedPropertyTypes = authzHelper.getAuthorizedPropertyTypes(
                ImmutableSet.of(entitySetId),
                requiredProperties,
                EnumSet.of(Permission.OWNER)
        ).getValue(entitySetId)
        if (!authorizedPropertyTypes.keys.containsAll(requiredProperties)) {
            throw ForbiddenException(
                    "You must be an owner of all required entity set properties to delete entities from it."
            )
        }

        return authorizedPropertyTypes
    }

    private fun deleteAuditEntitySetsForId(entitySetId: UUID) {
        val aclKey = AclKey(entitySetId)

        val propertyTypes = aresManager.auditingTypes.propertyTypes

        aresManager.getAuditRecordEntitySets(aclKey).forEach {
            dgm.deleteEntitySet(it, propertyTypes)
            edmManager.deleteEntitySet(it)
            securableObjectTypes.deleteSecurableObjectType(AclKey(it))
        }

        aresManager.getAuditEdgeEntitySets(aclKey).forEach {
            dgm.deleteEntitySet(it, mapOf())
            edmManager.deleteEntitySet(it)
            securableObjectTypes.deleteSecurableObjectType(AclKey(it))
        }

        aresManager.removeAuditRecordEntitySetConfiguration(aclKey)
    }

    private fun getCurrentUserId(): UUID {
        return spm.getPrincipal(Principals.getCurrentUser().id).id
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizations
    }

    private fun ensureEntitySetCanBeDeleted(entitySet: EntitySet) {
        val entitySetId = entitySet.id

        ensureObjectCanBeDeleted(entitySetId)

        if (entitySet.flags.contains(EntitySetFlag.AUDIT)) {
            throw ForbiddenException("You cannot delete entity set $entitySetId because it is an audit entity set.")
        }

    }
}