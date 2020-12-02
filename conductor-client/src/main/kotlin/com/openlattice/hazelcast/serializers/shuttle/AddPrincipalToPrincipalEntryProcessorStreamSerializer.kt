package com.openlattice.hazelcast.serializers.shuttle

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.AclKeyStreamSerializer
import com.openlattice.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.principals.AddPrincipalToPrincipalEntryProcessor
import org.springframework.stereotype.Component

@Component
class AddPrincipalToPrincipalEntryProcessorStreamSerializer : TestableSelfRegisteringStreamSerializer<AddPrincipalToPrincipalEntryProcessor> {
    override fun generateTestValue(): AddPrincipalToPrincipalEntryProcessor {
        return AddPrincipalToPrincipalEntryProcessor(TestDataFactory.aclKey())
    }

    override fun getClazz(): Class<out AddPrincipalToPrincipalEntryProcessor> {
        return AddPrincipalToPrincipalEntryProcessor::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ADD_PRINCIPAL_TO_PRINCIPAL_EP.ordinal
    }

    override fun read(`in`: ObjectDataInput): AddPrincipalToPrincipalEntryProcessor {
        return AddPrincipalToPrincipalEntryProcessor(AclKeyStreamSerializer.deserialize(`in`))
    }

    override fun write(out: ObjectDataOutput, `object`: AddPrincipalToPrincipalEntryProcessor) {
        AclKeyStreamSerializer.serialize(out, `object`.newAclKey)
    }


}