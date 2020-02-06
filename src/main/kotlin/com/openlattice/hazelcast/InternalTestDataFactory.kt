package com.openlattice.hazelcast

import com.openlattice.client.RetrofitFactory
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.shuttle.*
import org.apache.commons.text.CharacterPredicates
import org.apache.commons.text.RandomStringGenerator
import java.util.*

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
        fun integrationJob(): IntegrationJob {
            return IntegrationJob(randomAlphaNumeric.generate(5), IntegrationStatus.IN_PROGRESS)
        }
    }

}