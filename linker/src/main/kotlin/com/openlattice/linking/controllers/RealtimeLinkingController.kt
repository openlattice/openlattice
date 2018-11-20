package com.openlattice.linking.controllers

import com.openlattice.linking.LinkingQueryService
import com.openlattice.linking.RealtimeLinkingApi
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
import java.util.UUID
import java.util.Optional

@RestController
@RequestMapping(RealtimeLinkingApi.CONTROLLER)
class RealtimeLinkingController(
        private val lqs: LinkingQueryService,
        private val linkableTypes: Set<UUID>,
        private val entitySetBlacklist: Set<UUID>,
        private val whitelist: Optional<Set<UUID>>
) : RealtimeLinkingApi {
    companion object {
        private val logger = LoggerFactory.getLogger(RealtimeLinkingController::class.java)
    }

    @Override
    @RequestMapping(
            method = [RequestMethod.GET],
            consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getLinkingFinishedEntitySets(): Set<UUID> {
        val linkableEntitySets = lqs
                .getLinkableEntitySets(linkableTypes, entitySetBlacklist, whitelist.orElse(setOf()))
                .toSet()

        return lqs
                .getEntitiesNeedingLinking(linkableEntitySets)
                .groupBy({ it.first }) { it.second }
                .filter { it.value.isEmpty() }
                .keys
    }
}