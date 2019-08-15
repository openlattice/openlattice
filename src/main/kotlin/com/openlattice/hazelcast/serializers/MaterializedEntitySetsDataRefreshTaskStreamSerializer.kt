package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.assembler.tasks.MaterializedEntitySetsDataRefreshTask
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class MaterializedEntitySetsDataRefreshTaskStreamSerializer : SelfRegisteringStreamSerializer<MaterializedEntitySetsDataRefreshTask> {
    override fun getClazz(): Class<out MaterializedEntitySetsDataRefreshTask> {
        return MaterializedEntitySetsDataRefreshTask::class.java
    }

    override fun write(out: ObjectDataOutput?, `object`: MaterializedEntitySetsDataRefreshTask?) {
        return
    }

    override fun read(`in`: ObjectDataInput?): MaterializedEntitySetsDataRefreshTask{
        return MaterializedEntitySetsDataRefreshTask()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.MATERIALIZED_ENTITY_SETS_DATA_REFRESH_TASK.ordinal
    }

}