package com.openlattice.hazelcast.serializers

import com.google.common.collect.ImmutableSet
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.TestServer.Companion.testServer
import org.junit.Assert
import org.junit.Test
import org.slf4j.LoggerFactory

class SerializersTest {

    companion object {
        private val logger = LoggerFactory.getLogger(SerializersTest::class.java)

        private val excluded: Set<Class<Any>> = ImmutableSet.of()

        private var serializers: MutableCollection<TestableSelfRegisteringStreamSerializer<*>> = testServer.context.getBeansOfType(TestableSelfRegisteringStreamSerializer::class.java).values

        private var failed = false

        private fun test(tss: TestableSelfRegisteringStreamSerializer<Any>) {
            val expected = tss.generateTestValue()
            val ss1 = DefaultSerializationServiceBuilder().build()
            try {
                val dataOut: ObjectDataOutput = ss1.createObjectDataOutput(1)
                tss.write( dataOut, expected )
                val inputData = dataOut.toByteArray()
                val dataIn: ObjectDataInput = ss1.createObjectDataInput(inputData)
                val actual = tss.read(dataIn)
                if (expected != actual) {
                    logger.error("Incorrect serialization/deserialization of type\n {}:\n\tExpected {}\n\tbut got {}",
                            tss.clazz,
                            expected,
                            actual)
                    failed = true
                }
            } catch (e: Exception) {
                logger.error("Unable to serialize/deserialize type {}", tss.clazz, e)
                throw e
            }
        }
    }

    @Test
    fun testSerializers() {
        logger.info("Starting test stream")
        serializers.stream()
                .filter { !excluded.contains(it.clazz) }
                .forEach { test(it as TestableSelfRegisteringStreamSerializer<Any>) }
        Assert.assertFalse(failed)
    }

}
