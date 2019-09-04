package com.openlattice.linking

import com.hazelcast.aggregation.Aggregators
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.authorization.AceKey
import com.openlattice.authorization.AceValue
import com.openlattice.data.EntityDataKey
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.linking.mapstores.*
import com.openlattice.postgres.PostgresTable.LINKING_FEEDBACK
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import java.sql.ResultSet
import java.util.*
import java.util.function.Function
import java.util.function.Supplier

const val FETCH_SIZE = 100_000

class PostgresLinkingFeedbackService(private val hds: HikariDataSource, hazelcastInstance: HazelcastInstance) {

    private val linkingFeedbacks = hazelcastInstance.getMap<EntityKeyPair, Boolean>(HazelcastMap.LINKING_FEEDBACKS.name)

    fun addLinkingFeedback(entityLinkingFeedback: EntityLinkingFeedback) {
        linkingFeedbacks.set(entityLinkingFeedback.entityPair, entityLinkingFeedback.linked)
    }

    fun getLinkingFeedbacks(): Iterable<EntityLinkingFeedback> {
        // since we are not using an entryprocessor after retriveing all the feedbacks, it's easier to just query them
        return PostgresIterable(
                Supplier<StatementHolder> {
                    val connection = hds.connection
                    connection.autoCommit = false
                    val stmt = connection.prepareStatement(SELECT_ALL_SQL)
                    stmt.fetchSize = FETCH_SIZE
                    val rs = stmt.executeQuery()

                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, EntityLinkingFeedback> {
                    ResultSetAdapters.entityLinkingFeedback(it)
                }
        )
    }

    fun hasFeedbacks(feedbackType: FeedbackType, entity: EntityDataKey): Boolean {
        return buildPredicatesForQueryAndRun(feedbackType, entity) {
            linkingFeedbacks.aggregate( Aggregators.count(), it ) > 0
        }
    }

    fun getLinkingFeedbackEntityKeyPairs(feedbackType: FeedbackType, entity: EntityDataKey): Set<EntityKeyPair> {
        return buildPredicatesForQueryAndRun(feedbackType, entity) {
            linkingFeedbacks.keySet( it )
        }
    }

    fun getLinkingFeedbackOnEntity(feedbackType: FeedbackType, entity: EntityDataKey): Iterable<EntityLinkingFeedback> {
        return buildPredicatesForQueryAndRun(feedbackType, entity) {
            linkingFeedbacks.project(LinkingFeedbackProjection(), it)
        }
    }

    private fun <R> buildPredicatesForQueryAndRun(
            feedbackType: FeedbackType,
            entity: EntityDataKey,
            operation: ( predicates: Predicate<EntityKeyPair, Boolean> ) -> R ): R {
        return operation( buildPredicatesForQueries( feedbackType, entity ))
    }

    @SuppressWarnings("unchecked")
    fun buildPredicatesForQueries(feedbackType: FeedbackType, entity: EntityDataKey) : Predicate<EntityKeyPair, Boolean> {
        val entityPredicate = Predicates.or(
                Predicates.equal(FIRST_ENTITY_INDEX, entity),
                Predicates.equal(SECOND_ENTITY_INDEX, entity))

        val predicates = Predicates.and( entityPredicate,
            when( feedbackType ) {
                FeedbackType.Positive -> Predicates.equal(FEEDBACK_INDEX, true)
                FeedbackType.Negative -> Predicates.equal(FEEDBACK_INDEX, false)
                FeedbackType.All ->  Predicates.alwaysTrue<EntityKeyPair, Boolean>()
            }
        ) as Predicate<EntityKeyPair, Boolean>

        return predicates
    }

    fun getLinkingFeedback(entityPair: EntityKeyPair): EntityLinkingFeedback? {
        val feedback = linkingFeedbacks[entityPair] ?: return null

        return EntityLinkingFeedback(entityPair, feedback)
    }

    @SuppressWarnings("unchecked")
    fun deleteLinkingFeedbacks(entitySetId: UUID, entityKeyIds: Optional<Set<UUID>>): Int {
        val firstEntitySetPredicate = Predicates.equal(FIRST_ENTITY_SET_INDEX, entitySetId)
        val secondEntitySetPredicate = Predicates.equal(SECOND_ENTITY_SET_INDEX, entitySetId)

        val firstPredicate = if (entityKeyIds.isPresent) {
            Predicates.and(
                    firstEntitySetPredicate,
                    LinkingFeedbackPredicateBuilder.inUuidArray(entityKeyIds.get(), FIRST_ENTITY_KEY_INDEX))
        } else {
            firstEntitySetPredicate
        }

        val secondPredicate = if (entityKeyIds.isPresent) {
            Predicates.and(
                    secondEntitySetPredicate,
                    LinkingFeedbackPredicateBuilder.inUuidArray(entityKeyIds.get(), SECOND_ENTITY_KEY_INDEX))
        } else {
            secondEntitySetPredicate
        }

        val feedbackCount = linkingFeedbacks.count()
        linkingFeedbacks.removeAll(
                Predicates.or(firstPredicate, secondPredicate) as Predicate<EntityKeyPair, Boolean>)
        return feedbackCount - linkingFeedbacks.count()
    }

    fun deleteLinkingFeedback(entityPair: EntityKeyPair): Int {
        linkingFeedbacks.remove(entityPair)
        return 1
    }
}

private val SELECT_ALL_SQL = "SELECT * FROM ${LINKING_FEEDBACK.name}"