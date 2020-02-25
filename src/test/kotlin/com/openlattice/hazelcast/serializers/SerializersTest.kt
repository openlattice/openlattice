package com.openlattice.hazelcast.serializers

import com.google.common.collect.ImmutableSet
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.TestServer.Companion.testServer
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mockito.Mockito
import org.slf4j.LoggerFactory
import org.springframework.util.ClassUtils

@RunWith(Parameterized::class)
class SerializersTest(val serializer: TestableSelfRegisteringStreamSerializer<Any>, val _name: String) {

    companion object {
        private val logger = LoggerFactory.getLogger(SerializersTest::class.java)

        private val excluded: Set<Class<Any>> = ImmutableSet.of()

        @JvmStatic
        @Parameterized.Parameters(name = "{1}")
        fun getSerializers(): Array<Array<Any>> {
            val serializers: MutableCollection<TestableSelfRegisteringStreamSerializer<*>> = testServer.context.getBeansOfType(TestableSelfRegisteringStreamSerializer::class.java).values
            val acm = Mockito.mock(AssemblerConnectionManager::class.java)
            return serializers
                    .map { ss ->
                        if (ss is AssemblerConnectionManagerDependent<*>) {
                            ss.init(acm)
                        }
                        ss
                    }
                    .map { ss -> arrayOf(ss, ClassUtils.getUserClass(ss).simpleName) }
                    .toTypedArray()
        }
    }

    @Test
    fun testSerializer() {
        val expected = serializer.generateTestValue()
        val ss1 = DefaultSerializationServiceBuilder().build()
        try {
            val dataOut: ObjectDataOutput = ss1.createObjectDataOutput(1)
            serializer.write( dataOut, expected )
            val inputData = dataOut.toByteArray()
            val dataIn: ObjectDataInput = ss1.createObjectDataInput(inputData)
            val actual = serializer.read(dataIn)
            Assert.assertEquals(expected, actual)
        } catch (e: Exception) {
            logger.error("Unable to serialize/deserialize type {}", serializer.clazz, e)
            throw e
        }
    }

}
