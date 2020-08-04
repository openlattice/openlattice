package com.openlattice.hazelcast.serializers.listeners

import com.google.common.eventbus.EventBus
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.authorization.listeners.PermissionMapListener
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import org.springframework.stereotype.Component
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class PermissionMapListenerStreamSerializer : TestableSelfRegisteringStreamSerializer<PermissionMapListener> {
    @Inject
    private lateinit var eventBus: EventBus

    override fun generateTestValue(): PermissionMapListener = PermissionMapListener(EventBus())

    override fun getTypeId(): Int = StreamSerializerTypeIds.PERMISSION_MAP_LISTENER.ordinal

    override fun getClazz(): Class<out PermissionMapListener> {
        return PermissionMapListener::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: PermissionMapListener) {
        //Purposefully left blank.
    }

    override fun read(`in`: ObjectDataInput): PermissionMapListener {
        return PermissionMapListener(eventBus)
    }
}