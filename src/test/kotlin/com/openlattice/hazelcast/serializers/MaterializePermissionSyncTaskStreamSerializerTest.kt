package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.assembler.tasks.MaterializePermissionSyncTask

class MaterializePermissionSyncTaskStreamSerializerTest
    : AbstractStreamSerializerTest<MaterializePermissionSyncTaskStreamSerializer, MaterializePermissionSyncTask>() {

    override fun createSerializer(): MaterializePermissionSyncTaskStreamSerializer {
        return MaterializePermissionSyncTaskStreamSerializer()
    }

    override fun createInput(): MaterializePermissionSyncTask {
        return MaterializePermissionSyncTask()
    }
}