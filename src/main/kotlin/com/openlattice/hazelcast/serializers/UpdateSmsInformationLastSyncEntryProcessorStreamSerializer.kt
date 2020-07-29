package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organizations.processors.UpdateSmsInformationLastSyncEntryProcessor
import org.springframework.stereotype.Component
import java.time.OffsetDateTime

@Component
class UpdateSmsInformationLastSyncEntryProcessorStreamSerializer : TestableSelfRegisteringStreamSerializer<UpdateSmsInformationLastSyncEntryProcessor> {
    override fun generateTestValue(): UpdateSmsInformationLastSyncEntryProcessor {
        return UpdateSmsInformationLastSyncEntryProcessor(OffsetDateTime.now())
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_SMS_INFORMATION_LAST_SYNC_ENTRY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out UpdateSmsInformationLastSyncEntryProcessor> {
        return UpdateSmsInformationLastSyncEntryProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: UpdateSmsInformationLastSyncEntryProcessor) {
        OffsetDateTimeStreamSerializer.serialize(out, `object`.lastSync)
    }

    override fun read(`in`: ObjectDataInput): UpdateSmsInformationLastSyncEntryProcessor {
        return UpdateSmsInformationLastSyncEntryProcessor(OffsetDateTimeStreamSerializer.deserialize(`in`))
    }
}