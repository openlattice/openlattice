package com.openlattice.linking.controllers

import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.datastore.services.EdmManager
import com.openlattice.linking.*
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject
import kotlin.collections.HashSet

@RestController
@RequestMapping(RealtimeLinkingApi.CONTROLLER)
class RealtimeLinkingController(
        lc: LinkingConfiguration,
        edm: EdmManager
) : RealtimeLinkingApi, AuthorizingComponent {
    @Inject
    private lateinit var lqs: LinkingQueryService

    @Inject
    private lateinit var authz: AuthorizationManager

    private val entitySetBlacklist = lc.blacklist
    private val whitelist = lc.whitelist
    private val linkableTypes = edm.getEntityTypeUuids(lc.entityTypes)

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
                .getLinkableEntitySets(linkableTypes, entitySetBlacklist, whitelist.orElse(setOf()))
                .toSet()
        val entitySetsNeedLinking = lqs.getEntitiesNotLinked(linkableEntitySets).map { it.first }
        return linkableEntitySets.minus(entitySetsNeedLinking)
    }

    @RequestMapping(
            path = [RealtimeLinkingApi.MATCHED + RealtimeLinkingApi.LINKING_ID_PATH],
            method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getMatchedEntitiesForLinkingId(
            @PathVariable(RealtimeLinkingApi.LINKING_ID) linkingId: UUID
    ): Set<MatchedEntityPair> {
        ensureAdminAccess()

        val matches = lqs.getClusterFromLinkingId( linkingId )
        val matchedEntityPairs = HashSet<MatchedEntityPair>()

        matches.forEach {
            val first = it
            first.value.forEach {
                matchedEntityPairs.add(MatchedEntityPair(EntityKeyPair(first.key, it.key), it.value))
            }
        }

        return matchedEntityPairs
    }
}