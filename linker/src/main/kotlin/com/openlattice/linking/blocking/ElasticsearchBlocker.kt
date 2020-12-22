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
 *
 *
 */

package com.openlattice.linking.blocking

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Stopwatch
import com.google.common.base.Suppliers
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.conductor.rpc.ConductorElasticsearchApi
import com.openlattice.data.EntityDataKey
import com.openlattice.edm.EntitySet
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.linking.*
import com.openlattice.linking.util.PersonProperties
import com.openlattice.postgres.mapstores.EntityTypeMapstore
import com.openlattice.rhizome.hazelcast.DelegatedStringSet
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.streams.asSequence


private val logger = LoggerFactory.getLogger(ElasticsearchBlocker::class.java)

/**
 * Instantiates a blocking strategy that relies upon elasticsearch to compute initial blocks.
 */
@Component
class ElasticsearchBlocker(
        private val elasticsearch: ConductorElasticsearchApi,
        private val dataLoader: DataLoader,
        private val linkingFeedbackService: PostgresLinkingFeedbackService,
        hazelcast: HazelcastInstance
) : Blocker {

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap( hazelcast )
    private val entityTypes = HazelcastMap.ENTITY_TYPES.getMap( hazelcast )

    private val personEntityType = entityTypes.values(
            Predicates.equal(EntityTypeMapstore.FULLQUALIFIED_NAME_PREDICATE, PersonProperties.PERSON_TYPE_FQN.fullQualifiedNameAsString)
    ).first()

    @Deprecated("Unused")
    private val entitySetsCache = Suppliers
            .memoizeWithExpiration({
                entitySets.values.filter { it.entityTypeId == personEntityType.id }
                        .map(EntitySet::getId)
            }, 1000, TimeUnit.MILLISECONDS)

    @Timed
    override fun block(
            entityDataKey: EntityDataKey,
            entity: Optional<Map<UUID, Set<Any>>>,
            top: Int
    ): Pair<EntityDataKey, Map<EntityDataKey, Map<UUID, Set<Any>>>> {
        logger.info("Blocking for entity data key {}", entityDataKey)

        val sw = Stopwatch.createStarted()
        
        val existingEntitySetIds = entitySets.keys.toSet()
        var blockedEntitySetSearchResults = elasticsearch.executeBlockingSearch(
                personEntityType.id,
                getFieldSearches(entity.orElseGet { dataLoader.getEntity(entityDataKey) }),
                top,
                false
        ).filter { existingEntitySetIds.contains(it.key) }

        logger.info(
                "Entity data key {} blocked to {} elements in {} ms.", entityDataKey,
                blockedEntitySetSearchResults.values.map { it.size }.sum(),
                sw.elapsed(TimeUnit.MILLISECONDS)
        )

        val selfBlock: MutableSet<UUID>? = blockedEntitySetSearchResults[entityDataKey.entitySetId]
        if (selfBlock == null || !selfBlock.contains(entityDataKey.entityKeyId)) {
            logger.error("Entity {} did not block to itself.", entityDataKey)
            /*
             * We're going to assume there is something pathological about elements that do not block to themselves.
             * The main linking service will skip these elements anyway so we should avoid loading data related to
             * pathological cases
             */
            /* There can be cases, when there is no sufficient data for an entity to be blocked to itself
             * (example: only 1 property has value)
             * If it cannot block to itself, we add link it to itself
             */
            blockedEntitySetSearchResults = mutableMapOf(
                    entityDataKey.entitySetId to mutableSetOf(entityDataKey.entityKeyId))
        }

        sw.reset()
        sw.start()

        val loadedData = entityDataKey to
                removeNegativeFeedbackFromSearchResult(entityDataKey, blockedEntitySetSearchResults)
                        .filter { it.value.isNotEmpty() }
                        .entries
                        .parallelStream()
                        .flatMap { entry ->
                            dataLoader
                                    .getEntityStream(entry.key, entry.value)
                                    .stream()
                                    .map { EntityDataKey(entry.key, it.first) to it.second }
                        }
                        .asSequence()
                        .toMap()

        logger.info(
                "Loading {} entities took {} ms.", loadedData.second.values.map { it.size }.sum(),
                sw.elapsed(TimeUnit.MILLISECONDS)
        )

        return loadedData

    }

    /**
     * Handles rendering an object into field searches for blocking.
     */
    private fun getFieldSearches(entity: Map<UUID, Set<Any>>): Map<UUID, DelegatedStringSet> {
        return entity.map { it.key to DelegatedStringSet.wrap(it.value.map { it.toString() }.toSet()) }.toMap()
    }

    private fun removeNegativeFeedbackFromSearchResult(
            entity: EntityDataKey, searchResult: Map<UUID, Set<UUID>>
    ): Map<UUID, Set<UUID>> {
        val negFeedbacks = linkingFeedbackService.getLinkingFeedbackEntityKeyPairs(FeedbackType.Negative, entity)
        return searchResult.mapValues {
            it.value.filter {
                // remove pairs which have feedbacks for not matching this entity
                entityKeyId ->
                !negFeedbacks.contains(EntityKeyPair(entity, EntityDataKey(it.key, entityKeyId)))
            }.toSet()
        }
    }
}
