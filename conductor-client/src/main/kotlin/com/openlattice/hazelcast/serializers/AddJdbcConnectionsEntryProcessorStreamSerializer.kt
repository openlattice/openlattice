package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.external.AddJdbcConnectionsEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
@SuppressFBWarnings(value = ["OBJECT_DESERIALIZATION"], justification = "Trust org EP serialization")
class AddJdbcConnectionsEntryProcessorStreamSerializer : SelfRegisteringStreamSerializer<AddJdbcConnectionsEntryProcessor> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.JDBC_CONNECTIONS_ENTRY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out AddJdbcConnectionsEntryProcessor> = AddJdbcConnectionsEntryProcessor::class.java

    override fun write(out: ObjectDataOutput, obj: AddJdbcConnectionsEntryProcessor) {
        JdbcConnectionsStreamSerializer.serialize(out, obj.jdbcConnections)
    }

    override fun read(input: ObjectDataInput): AddJdbcConnectionsEntryProcessor {
        return AddJdbcConnectionsEntryProcessor(JdbcConnectionsStreamSerializer.deserialize(input))
    }
}