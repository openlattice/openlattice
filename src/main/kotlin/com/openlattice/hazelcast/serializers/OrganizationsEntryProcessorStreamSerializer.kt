package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organizations.processors.OrganizationEntryProcessor
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class OrganizationsEntryProcessorStreamSerializer : SelfRegisteringStreamSerializer<OrganizationEntryProcessor> {
    companion object {
        private val logger = LoggerFactory.getLogger(OrganizationsEntryProcessorStreamSerializer::class.java)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ORGANIZATION_ENTRY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out OrganizationEntryProcessor> {
        return OrganizationEntryProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, obj: OrganizationEntryProcessor) {
        val baos = ByteArrayOutputStream()
        val oos = ObjectOutputStream(baos)
        oos.writeObject(obj)
        oos.close()
        oos.flush()
        baos.close()

        out.writeByteArray(baos.toByteArray())
    }

    override fun read(input: ObjectDataInput): OrganizationEntryProcessor {
        val bais = ByteArrayInputStream(input.readByteArray())
        val ois = ObjectInputStream(bais)
        return try {
            ois.readObject() as OrganizationEntryProcessor
        } catch (e: ClassNotFoundException) {
            logger.error("Unable to deserialize object because class not found. ", e)
            OrganizationEntryProcessor { org ->
                LoggerFactory.getLogger(OrganizationEntryProcessor::class.java)
                        .error("This entry processor didn't de-serialize correctly.")
            }
        }

    }
}