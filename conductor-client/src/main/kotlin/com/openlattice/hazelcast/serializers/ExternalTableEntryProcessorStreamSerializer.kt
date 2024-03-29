package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.processors.organizations.ExternalTableEntryProcessor
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

@Component
@SuppressFBWarnings(value = ["OBJECT_DESERIALIZATION"], justification = "Trust external table EP serialization")
class ExternalTableEntryProcessorStreamSerializer : SelfRegisteringStreamSerializer<ExternalTableEntryProcessor> {
    companion object {
        private val logger = LoggerFactory.getLogger(ExternalTableEntryProcessorStreamSerializer::class.java)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.EXTERNAL_TABLE_EP.ordinal
    }

    override fun getClazz(): Class<out ExternalTableEntryProcessor> {
        return ExternalTableEntryProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, obj: ExternalTableEntryProcessor) {
        val baos = ByteArrayOutputStream()
        val oos = ObjectOutputStream(baos)
        oos.writeObject(obj)
        oos.close()
        oos.flush()
        baos.close()

        out.writeByteArray(baos.toByteArray())
    }

    override fun read(input: ObjectDataInput): ExternalTableEntryProcessor {
        val bais = ByteArrayInputStream(input.readByteArray()!!)
        val ois = ObjectInputStream(bais)
        return try {
            ois.readObject() as ExternalTableEntryProcessor
        } catch (e: ClassNotFoundException) {
            logger.error("Unable to deserialize object because class not found. ", e)
            ExternalTableEntryProcessor {
                LoggerFactory.getLogger(ExternalTableEntryProcessor::class.java)
                        .error("This entry processor didn't deserialize correctly.")
                ExternalTableEntryProcessor.Result(null, false)
            }
        }

    }
}