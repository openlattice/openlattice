package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.assembler.processors.InitializeOrganizationAssemblyProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import org.springframework.stereotype.Component
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

@Component
class InitializeOrganizationAssemblyProcessorStreamSerializer
    : TestableSelfRegisteringStreamSerializer<InitializeOrganizationAssemblyProcessor>,
        AssemblerConnectionManagerDependent<Void?> {
    private lateinit var acm: AssemblerConnectionManager

    override fun getClazz(): Class<InitializeOrganizationAssemblyProcessor> {
        return InitializeOrganizationAssemblyProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput): InitializeOrganizationAssemblyProcessor {
        return InitializeOrganizationAssemblyProcessor(`in`.readUTF()).init(acm)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.INITIALIZE_ORGANIZATION_ASSEMBLY_PROCESSOR.ordinal
    }

    override fun init(acm: AssemblerConnectionManager): Void? {
        this.acm = acm
        return null
    }

    override fun generateTestValue(): InitializeOrganizationAssemblyProcessor {
        return InitializeOrganizationAssemblyProcessor(
                ExternalDatabaseConnectionManager.buildDefaultOrganizationDatabaseName( UUID.randomUUID() )
        )
    }

    override fun write(out: ObjectDataOutput, `object`: InitializeOrganizationAssemblyProcessor) {
        out.writeUTF(`object`.dbName)
    }
}