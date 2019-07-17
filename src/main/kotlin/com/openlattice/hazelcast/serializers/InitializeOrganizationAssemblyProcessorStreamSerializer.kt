package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.processors.InitializeOrganizationAssemblyProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

@Component
class InitializeOrganizationAssemblyProcessorStreamSerializer : SelfRegisteringStreamSerializer<InitializeOrganizationAssemblyProcessor>, AssemblerConnectionManagerDependent {
    private lateinit var acm: AssemblerConnectionManager

    override fun getClazz(): Class<InitializeOrganizationAssemblyProcessor> {
        return InitializeOrganizationAssemblyProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, obj: InitializeOrganizationAssemblyProcessor) {
        OrganizationStreamSerializer.serialize(out, obj.organization)
    }

    override fun read(input: ObjectDataInput): InitializeOrganizationAssemblyProcessor {
        val organization = OrganizationStreamSerializer.deserialize(input)
        return InitializeOrganizationAssemblyProcessor(organization).init(acm)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.INITIALIZE_ORGANIZATION_ASSEMBLY_PROCESSOR.ordinal
    }

    override fun init(assemblerConnectionManager: AssemblerConnectionManager) {
        this.acm = assemblerConnectionManager
    }
}