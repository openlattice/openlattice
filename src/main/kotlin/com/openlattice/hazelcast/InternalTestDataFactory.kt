package com.openlattice.hazelcast

import com.geekbeast.rhizome.hazelcast.DelegatedIntList
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AclKeySet
import com.openlattice.client.RetrofitFactory
import com.openlattice.codex.SendCodexMessageTask
import com.openlattice.collections.CollectionTemplates
import com.openlattice.hazelcast.serializers.AclKeyStreamSerializer
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.notifications.sms.SubscriptionNotification
import com.openlattice.scheduling.ScheduledTask
import com.openlattice.shuttle.Flight
import com.openlattice.shuttle.FlightPlanParameters
import com.openlattice.shuttle.Integration
import com.openlattice.shuttle.IntegrationJob
import com.openlattice.shuttle.IntegrationStatus
import org.apache.commons.text.CharacterPredicates
import org.apache.commons.text.RandomStringGenerator
import java.time.OffsetDateTime
import java.util.*
import kotlin.random.Random

class InternalTestDataFactory {
    companion object {
        private val allowedLetters = arrayOf(charArrayOf('a', 'z'), charArrayOf('A', 'Z'))
        private val allowedDigitsAndLetters = arrayOf(charArrayOf('a', 'z'), charArrayOf('A', 'Z'), charArrayOf('0', '9'))
        private val random = RandomStringGenerator.Builder()
                .build()
        private val randomAlpha = RandomStringGenerator.Builder()
                .withinRange(*allowedLetters)
                .filteredBy(CharacterPredicates.LETTERS)
                .build()
        private val randomAlphaNumeric = RandomStringGenerator.Builder()
                .withinRange(*allowedDigitsAndLetters)
                .filteredBy(CharacterPredicates.LETTERS, CharacterPredicates.DIGITS)
                .build()

        @JvmStatic
        fun integration(): Integration {
            val key = UUID.randomUUID()
            val environment = RetrofitFactory.Environment.LOCAL
            val s3bucket = random.generate(10)
            val contacts = setOf<String>(random.generate(10))
            val organizationId = UUID.randomUUID()
            return Integration(
                    key,
                    environment,
                    s3bucket,
                    contacts,
                    organizationId,
                    Optional.empty(),
                    Optional.of(5),
                    Optional.empty(),
                    mutableMapOf(randomAlphaNumeric.generate(5) to flightPlanParameters())
            )
        }

        @JvmStatic
        fun flightPlanParameters(): FlightPlanParameters {
            val sql = randomAlphaNumeric.generate(5)
            val source = mapOf<String, String>()
            val sourcePrimaryKeyColumns = listOf(randomAlphaNumeric.generate(5))
            val flight = Flight(
                    emptyMap(),
                    Optional.empty(),
                    Optional.of(emptyMap()),
                    Optional.of(TestDataFactory.random(5)),
                    Optional.empty(),
                    Optional.of(UUID.randomUUID()))
            return FlightPlanParameters(
                    sql,
                    source,
                    sourcePrimaryKeyColumns,
                    null,
                    flight
            )
        }

        @JvmStatic
        fun collectionTemplates(): CollectionTemplates {
            val outer = mutableMapOf<UUID, MutableMap<UUID, UUID>>()
            for (j in 0 until 10 ) {
                val inner = mutableMapOf<UUID, UUID>()
                for ( i in 0 until 10 ){
                    inner.put(UUID.randomUUID(), UUID.randomUUID())
                }
                outer.put(UUID.randomUUID(), inner)
            }
            return CollectionTemplates( outer )
        }

        @JvmStatic
        fun delegatedIntList(): DelegatedIntList {
            return DelegatedIntList(
                    listOf(
                            Random.nextInt(), Random.nextInt(), Random.nextInt(), Random.nextInt(), Random.nextInt(),
                            Random.nextInt(), Random.nextInt(), Random.nextInt(), Random.nextInt(), Random.nextInt()
                    )
            )
        }

        @JvmStatic
        fun aclKeySet(): AclKeySet {
            val set = AclKeySet(7)
            set.add(AclKeyStreamSerializer().generateTestValue() as AclKey)
            set.add(AclKeyStreamSerializer().generateTestValue() as AclKey)
            set.add(AclKeyStreamSerializer().generateTestValue() as AclKey)
            set.add(AclKeyStreamSerializer().generateTestValue() as AclKey)
            set.add(AclKeyStreamSerializer().generateTestValue() as AclKey)
            set.add(AclKeyStreamSerializer().generateTestValue() as AclKey)
            set.add(AclKeyStreamSerializer().generateTestValue() as AclKey)
            return set
        }

        @JvmStatic
        fun integrationJob(): IntegrationJob {
            return IntegrationJob(randomAlphaNumeric.generate(5), IntegrationStatus.IN_PROGRESS)
        }

        @JvmStatic
        fun subscriptionNotification(): SubscriptionNotification {
            return SubscriptionNotification(TestDataFactory.randomAlphanumeric(20), TestDataFactory.randomAlphanumeric(10))
        }

        fun scheduledTask(id: UUID = UUID.randomUUID()): ScheduledTask {
            return ScheduledTask(id, OffsetDateTime.now(), sendCodexMessageTask())
        }

        fun sendCodexMessageTask(): SendCodexMessageTask {
            return SendCodexMessageTask(TestDataFactory.messageRequest())
        }
    }

}