package com.openlattice.datastore.data.controllers

import com.codahale.metrics.annotation.Timed
import com.google.common.collect.HashMultimap
import com.google.common.collect.SetMultimap
import com.openlattice.ApiHelpers
import com.openlattice.EntityKeyGenerationBundle
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.authorization.EdmAuthorizationHelper
import com.openlattice.data.DataGraphManager
import com.openlattice.data.DataIntegrationApi
import com.openlattice.data.EntityKey
import com.openlattice.data.integration.S3EntityData
import com.openlattice.data.storage.aws.AwsDataSinkService
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(DataIntegrationApi.CONTROLLER)
class DataIntegrationController : DataIntegrationApi, AuthorizingComponent {
    private val encoder = Base64.getEncoder()

    @Inject
    private lateinit var dgm: DataGraphManager

    @Inject
    private lateinit var awsDataSinkService: AwsDataSinkService

    @Inject
    private lateinit var authz: AuthorizationManager

    @Inject
    private lateinit var authzHelper: EdmAuthorizationHelper

    override fun getAuthorizationManager(): AuthorizationManager {
        return authz
    }

    override fun generatePresignedUrls(data: Collection<S3EntityData?>?): List<String?>? {
        throw UnsupportedOperationException("This shouldn't be invoked. Just here for the interface and efficiency")
    }

    @Timed
    @PostMapping("/" + DataIntegrationApi.S3)
    fun generatePresignedUrls(
        @RequestBody data: List<S3EntityData>
    ): List<String> {
        val entitySetIds = data.map(S3EntityData::entitySetId).toSet()
        val propertyIdsByEntitySet: SetMultimap<UUID, UUID> = HashMultimap.create()
        data.forEach { (entitySetId, _, propertyTypeId) ->
            propertyIdsByEntitySet.put(entitySetId, propertyTypeId)
        }

        //Ensure that we have read access to entity set metadata.
        entitySetIds.forEach { entitySetId: UUID -> ensureReadAccess(AclKey(entitySetId)) }
        accessCheck(
            EdmAuthorizationHelper.aclKeysForAccessCheck(
                propertyIdsByEntitySet,
                EdmAuthorizationHelper.WRITE_PERMISSION
            )
        )
        val authorizedPropertyTypes = entitySetIds.associateWith { entitySetId ->
            authzHelper.getAuthorizedPropertyTypes(entitySetId, EdmAuthorizationHelper.WRITE_PERMISSION)
        }

        return awsDataSinkService.generatePresignedUrls(data, authorizedPropertyTypes)
    }

    //Just sugar to conform to API interface. While still allow efficient serialization.
    override fun getEntityKeyIds(entityKeys: Set<EntityKey>): List<UUID> {
        throw UnsupportedOperationException("Nobody should be calling this.")
    }

    @Timed
    @PostMapping(
        "/" + DataIntegrationApi.ENTITY_KEY_IDS,
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    fun getEntityKeyIds(@RequestBody entityKeys: LinkedHashSet<EntityKey>): Set<UUID> {
        val entitySetIds = entityKeys.map { it.entitySetId }.toSet()
        entitySetIds.forEach { entitySetId: UUID -> ensureWriteAccess(AclKey(entitySetId)) }
        return dgm.getEntityKeyIds(entityKeys)
    }

    @Timed
    @PostMapping(
        "/" + DataIntegrationApi.ENTITY_KEYS,
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun generateEntityKeys(@RequestBody bundle: EntityKeyGenerationBundle): Map<UUID, EntityKey> {
        val entityKeys = bundle.entities.map {
            EntityKey(bundle.entitySetId, ApiHelpers.generateDefaultEntityId(bundle.keyPropertyTypeIds, it))
        }.toSet()
        val ids = getEntityKeyIds(entityKeys)
        return ids.zip(entityKeys).toMap()
    }
}