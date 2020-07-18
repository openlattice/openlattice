package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.codex.CodexMessageSyncTask
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class CodexMessageSyncTaskStreamSerializer : NoOpSelfRegisteringStreamSerializer<CodexMessageSyncTask>() {

    override fun getClazz(): Class<out CodexMessageSyncTask> {
        return CodexMessageSyncTask::class.java
    }

    override fun read(`in`: ObjectDataInput?): CodexMessageSyncTask {
        return CodexMessageSyncTask()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.CODEX_MESSAGE_SYNC_TASK.ordinal
    }
}