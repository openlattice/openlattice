package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.assembler.tasks.MaterializePermissionSyncTask
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class MaterializePermissionSyncTaskStreamSerializer : NoOpSelfRegisteringStreamSerializer<MaterializePermissionSyncTask>() {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.MATERIALIZE_PERMISSIONS_SYNC_TASK.ordinal
    }

    override fun getClazz(): Class<out MaterializePermissionSyncTask> {
        return MaterializePermissionSyncTask::class.java
    }

    override fun read(`in`: ObjectDataInput?): MaterializePermissionSyncTask {
        return MaterializePermissionSyncTask()
    }

}