package com.openlattice.hazelcast.serializers

import com.esotericsoftware.kryo.DefaultSerializer
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.ClosureSerializer
import com.esotericsoftware.kryo.serializers.DefaultSerializers
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.serializers.AclKeyKryoSerializer
import com.openlattice.authorization.serializers.EntityDataLambdasStreamSerializer
import com.openlattice.conductor.rpc.*
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organizations.Organization
import com.openlattice.organizations.PrincipalSet
import com.openlattice.organizations.processors.OrganizationEntryProcessor
import com.openlattice.organizations.serializers.DelegatedStringSetKryoSerializer
import com.openlattice.organizations.serializers.DelegatedUUIDSetKryoSerializer
import com.openlattice.organizations.serializers.PrincipalSetKryoSerializer
import com.openlattice.rhizome.hazelcast.DelegatedStringSet
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet
import com.openlattice.search.requests.SearchConstraints
import org.objenesis.strategy.StdInstantiatorStrategy
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.lang.invoke.SerializedLambda
import java.util.function.Consumer
import java.util.function.Function

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

    override fun read(input: ObjectDataInput): OrganizationEntryProcessor? {
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