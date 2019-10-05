package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.assembler.tasks.MaterializePermissionSyncTask
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class MaterializePermissionSyncTaskStreamSerializer : SelfRegisteringStreamSerializer<MaterializePermissionSyncTask> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.MATERIALIZE_PERMISSIONS_SYNC_TASK.ordinal
    }

    override fun getClazz(): Class<out MaterializePermissionSyncTask> {
        return MaterializePermissionSyncTask::class.java
    }

    override fun write(out: ObjectDataOutput?, `object`: MaterializePermissionSyncTask?) {}

    override fun read(`in`: ObjectDataInput?): MaterializePermissionSyncTask {
        return MaterializePermissionSyncTask()
    }

}