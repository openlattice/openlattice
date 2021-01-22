package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.external.JdbcConnections
import com.openlattice.external.JdbcConnectionsEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.processors.EpResult
import com.openlattice.organizations.processors.OrganizationEntryProcessor
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
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
@SuppressFBWarnings(value = ["OBJECT_DESERIALIZATION"], justification = "Trust org EP serialization")
class JdbcConnectionsEntryProcessorStreamSerializer : SelfRegisteringStreamSerializer<JdbcConnectionsEntryProcessor> {
    companion object {
        private val logger = LoggerFactory.getLogger(JdbcConnectionsEntryProcessorStreamSerializer::class.java)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.JDBC_CONNECTIONS_ENTRY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out JdbcConnectionsEntryProcessor> = JdbcConnectionsEntryProcessor::class.java


    override fun write(out: ObjectDataOutput, obj: JdbcConnectionsEntryProcessor) {
        JdbcConnectionsStreamSerializer.serialize(out, obj.jdbcConnections)

        val baos = ByteArrayOutputStream()
        val oos = ObjectOutputStream(baos)
        oos.writeObject(obj.update)
        oos.close()
        oos.flush()
        baos.close()

        out.writeByteArray(baos.toByteArray())
    }

    override fun read(input: ObjectDataInput): JdbcConnectionsEntryProcessor {
        val jdbcConnections = JdbcConnectionsStreamSerializer.deserialize(input)
        val bais = ByteArrayInputStream(input.readByteArray())
        val ois = ObjectInputStream(bais)
        return try {
            JdbcConnectionsEntryProcessor( jdbcConnections, ois.readObject() as (JdbcConnections) -> EpResult)
        } catch (e: ClassNotFoundException) {
            logger.error("Unable to deserialize object because class not found. ", e)
            JdbcConnectionsEntryProcessor(JdbcConnections()) { org ->
                LoggerFactory.getLogger(JdbcConnectionsEntryProcessor::class.java)
                        .error("This entry processor didn't de-serialize correctly.")
                EpResult(null, false)
            }
        }

    }
}