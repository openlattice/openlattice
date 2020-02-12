package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organizations.processors.UpdateOrganizationSmsEntitySetInformationEntryProcessor
import org.springframework.stereotype.Component

@Component
class UpdateOrganizationSmsEntitySetInformationEntryProcessorStreamSerializer : TestableSelfRegisteringStreamSerializer<UpdateOrganizationSmsEntitySetInformationEntryProcessor> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_ORGANIZATION_SMS_ENTITY_SET_INFORMATION_ENTRY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out UpdateOrganizationSmsEntitySetInformationEntryProcessor> {
        return UpdateOrganizationSmsEntitySetInformationEntryProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: UpdateOrganizationSmsEntitySetInformationEntryProcessor) {
        out.writeInt(`object`.smsEntitySetInformation.size)
        `object`.smsEntitySetInformation.forEach { SmsEntitySetInformationStreamSerializer.serialize(out, it) }
    }

    override fun read(`in`: ObjectDataInput): UpdateOrganizationSmsEntitySetInformationEntryProcessor {
        val size = `in`.readInt()
        return UpdateOrganizationSmsEntitySetInformationEntryProcessor(
                (0 until size).map { SmsEntitySetInformationStreamSerializer.deserialize(`in`) }
        )
    }

    override fun generateTestValue(): UpdateOrganizationSmsEntitySetInformationEntryProcessor {
        return UpdateOrganizationSmsEntitySetInformationEntryProcessor(listOf(TestDataFactory.smsEntitySetInformation(), TestDataFactory.smsEntitySetInformation()))
    }
}