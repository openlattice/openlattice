package com.openlattice.linking.controllers

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheBuilderSpec
import com.google.common.cache.CacheLoader
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.data.DataGraphManager
import com.openlattice.data.EntityDataKey
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EdmService
import com.openlattice.linking.*
import com.openlattice.linking.blocking.Blocker
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject
import kotlin.collections.HashSet

@RestController
@RequestMapping(RealtimeLinkingApi.CONTROLLER)
class RealtimeLinkingController(
        lc: LinkingConfiguration,
        edm: EdmManager
) : RealtimeLinkingApi, AuthorizingComponent {
    companion object {
        private val logger = LoggerFactory.getLogger(RealtimeLinkingController::class.java)
    }
    @Inject
    private lateinit var blocker: Blocker

    @Inject
    private lateinit var lqs: LinkingQueryService

    @Inject
    private lateinit var authz: AuthorizationManager

    @Inject
    private lateinit var dgm: DataGraphManager

    @Inject
    private lateinit var edmService: EdmService

    private val entitySetBlacklist = lc.blacklist
    private val priorityEntitySets = lc.whitelist.orElseGet { setOf() }
    private val linkableTypes = edm.getEntityTypeUuids(lc.entityTypes)

    private val propertyFqnCache = CacheBuilder
            .newBuilder()
            .maximumSize(8192)
            .build(CacheLoader.from { propertyTypeId: UUID? -> edmService.getPropertyTypeFqn(propertyTypeId!!) })

    override fun getAuthorizationManager(): AuthorizationManager {
        return authz
    }

    @SuppressFBWarnings(
            value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"],
            justification = "lateinit prevents NPE here"
    )
    @RequestMapping(
            path = [RealtimeLinkingApi.FINISHED + RealtimeLinkingApi.SET],
            method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getLinkingFinishedEntitySets(): Set<UUID> {
        ensureAdminAccess()
        val linkableEntitySets = lqs
                .getLinkableEntitySets(linkableTypes, entitySetBlacklist, priorityEntitySets)
                .toSet()
        val entitySetsNeedLinking = lqs.getEntitiesNotLinked(linkableEntitySets).map { it.first }
        return linkableEntitySets.minus(entitySetsNeedLinking)
    }

    @RequestMapping(
            path = [RealtimeLinkingApi.MATCHED + RealtimeLinkingApi.LINKING_ID_PATH],
            method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getMatchedEntitiesForLinkingId(
            @PathVariable(RealtimeLinkingApi.LINKING_ID) linkingId: UUID
    ): Set<MatchedEntityPair> {
        ensureAdminAccess()

        val matches = lqs.getClusterFromLinkingId(linkingId)
        val matchedEntityPairs = HashSet<MatchedEntityPair>()

        matches.forEach {
            val first = it
            first.value.forEach {
                matchedEntityPairs.add(MatchedEntityPair(EntityKeyPair(first.key, it.key), it.value))
            }
        }

        return matchedEntityPairs
    }

    @RequestMapping(
            path = [RealtimeLinkingApi.BLOCKING],
            method = [RequestMethod.POST],
            consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun block(
            @RequestBody blockingRequest: BlockingRequest
    ): Map<UUID, Map<UUID, List<BlockedEntity>>> {
        ensureAdminAccess()

        return blockingRequest.entities.mapValues { (entitySetId, entityKeyIds) ->
            entityKeyIds.associateWith { entityKeyId ->
                val blockedEntities = blocker
                        .block(EntityDataKey(entitySetId, entityKeyId),top = blockingRequest.blockSize)
                        .entities
                        logger.info( "Blocked entities raw: {} ", blockedEntities )
                        blockedEntities
                        .map { (edk, entity) ->
                            BlockedEntity(
                                    edk,
                                    entity.mapKeys { propertyFqnCache[it.key]!! }
                            )
                        }
            }
        }
    }

    override fun createNewLinkedEntity(entityDataKeys: MutableSet<EntityDataKey>?): UUID {
        TODO("Not yet implemented")
    }

    override fun getLinkedEntityKeyIds(linkingEntitySetId: UUID?): MutableMap<UUID, MutableSet<EntityDataKey>> {
        TODO("Not yet implemented")
    }

    override fun setLinkedEntities(
            linkingEntitySetId: UUID?, linkedEntityKeyId: UUID?, entityDataKeys: MutableSet<EntityDataKey>?
    ): Int {
        TODO("Not yet implemented")
    }

    override fun addLinkedEntities(
            linkingEntitySetId: UUID?, linkedEntityKeyId: UUID?, entityDataKeys: MutableSet<EntityDataKey>?
    ): MutableSet<EntityDataKey> {
        TODO("Not yet implemented")
    }

    override fun removeLinkedEntities(
            linkingEntitySetId: UUID?, linkedEntityKeyId: UUID?, entityDataKeys: MutableSet<EntityDataKey>?
    ): MutableSet<EntityDataKey> {
        TODO("Not yet implemented")
    }
}