package com.openlattice.hazelcast.serializers

import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.transporter.tasks.TransporterRunSyncTask
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@Component
class TransporterRunSyncTaskStreamSerializer: NoOpSelfRegisteringStreamSerializer<TransporterRunSyncTask>() {
    override fun getClazz(): Class<out TransporterRunSyncTask> {
        return TransporterRunSyncTask::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.TRANSPORTER_RUN_SYNC_TASK.ordinal
    }
}