package com.openlattice.hazelcast.serializers

import com.esotericsoftware.kryo.io.ByteBufferInputStream
import com.esotericsoftware.kryo.io.ByteBufferOutputStream
import com.google.common.collect.ImmutableSet
import com.hazelcast.internal.serialization.InputOutputFactory
import com.hazelcast.internal.serialization.InternalSerializationService
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream
import com.hazelcast.internal.serialization.impl.SerializationServiceV1
import com.hazelcast.nio.BufferObjectDataInput
import com.hazelcast.nio.BufferObjectDataOutput
import com.hazelcast.nio.serialization.Data
import com.openlattice.TestServer.Companion.testServer
import org.junit.Test
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.nio.ByteOrder

class SerializersTest {

    companion object {
        private val logger = LoggerFactory.getLogger(SerializersTest::class.java)

        private val excluded: Set<Class<Any>> = ImmutableSet.of()

        private var serializers: MutableCollection<TestableSelfRegisteringStreamSerializer<*>> = testServer.context.getBeansOfType(TestableSelfRegisteringStreamSerializer::class.java).values

        private val failed = mutableSetOf<Class<Any>>()

        private fun test(tss: TestableSelfRegisteringStreamSerializer<Any>) {
            val expected = tss.generateTestValue()
            val ss1 = SerializationServiceV1.builder()
                    .withInputOutputFactory(object: InputOutputFactory {
                        override fun createInput(data: Data?, service: InternalSerializationService?): BufferObjectDataInput {
                            TODO("don't need") //To change body of created functions use File | Settings | File Templates.
                        }

                        override fun createInput(buffer: ByteArray?, service: InternalSerializationService?): BufferObjectDataInput {
                            TODO("don't need") //To change body of created functions use File | Settings | File Templates.
                        }

                        override fun getByteOrder(): ByteOrder {
                            return ByteOrder.nativeOrder()
                        }

                        override fun createOutput(size: Int, service: InternalSerializationService?): BufferObjectDataOutput {
                            TODO("don't need") //To change body of created functions use File | Settings | File Templates.
                        }
                    })
                    .build()
            val buffer = ByteBuffer.allocate(4096)
            val out = ObjectDataOutputStream( ByteBufferOutputStream(buffer), ss1)
            val input = ObjectDataInputStream( ByteBufferInputStream(buffer), ss1)
            try {
                tss.write( out, expected )
                val actual = tss.read(input)
                if (expected != actual) {
                    logger.error("Incorrect serialization/deserialization of type\n {}:\n\tExpected {}\n\tbut got {}",
                            tss.clazz,
                            expected,
                            actual)
                }
//                Assert.assertEquals( expected, actual)
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
        failed.stream()
                .forEach { }

    }

}
