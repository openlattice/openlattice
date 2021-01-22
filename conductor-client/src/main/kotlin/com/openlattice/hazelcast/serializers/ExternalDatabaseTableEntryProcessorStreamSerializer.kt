package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.processors.organizations.ExternalDatabaseTableEntryProcessor
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

@Component
@SuppressFBWarnings(value = ["OBJECT_DESERIALIZATION"], justification = "Trust external table EP serialization")
class ExternalDatabaseTableEntryProcessorStreamSerializer : SelfRegisteringStreamSerializer<ExternalDatabaseTableEntryProcessor> {
    companion object {
        private val logger = LoggerFactory.getLogger(ExternalDatabaseTableEntryProcessorStreamSerializer::class.java)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.EXTERNAL_DATABASE_TABLE_EP.ordinal
    }

    override fun getClazz(): Class<out ExternalDatabaseTableEntryProcessor> {
        return ExternalDatabaseTableEntryProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, obj: ExternalDatabaseTableEntryProcessor) {
        val baos = ByteArrayOutputStream()
        val oos = ObjectOutputStream(baos)
        oos.writeObject(obj)
        oos.close()
        oos.flush()
        baos.close()

        out.writeByteArray(baos.toByteArray())
    }

    override fun read(input: ObjectDataInput): ExternalDatabaseTableEntryProcessor {
        val bais = ByteArrayInputStream(input.readByteArray())
        val ois = ObjectInputStream(bais)
        return try {
            ois.readObject() as ExternalDatabaseTableEntryProcessor
        } catch (e: ClassNotFoundException) {
            logger.error("Unable to deserialize object because class not found. ", e)
            ExternalDatabaseTableEntryProcessor {
                LoggerFactory.getLogger(ExternalDatabaseTableEntryProcessor::class.java)
                        .error("This entry processor didn't deserialize correctly.")
                ExternalDatabaseTableEntryProcessor.Result(null, false)
            }
        }

    }
}