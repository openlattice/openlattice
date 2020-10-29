package com.openlattice.authorization.listeners

import com.google.common.eventbus.EventBus
import com.hazelcast.core.EntryEvent
import com.hazelcast.map.listener.EntryAddedListener
import com.hazelcast.map.listener.EntryRemovedListener
import com.hazelcast.map.listener.EntryUpdatedListener
import com.openlattice.assembler.events.MaterializePermissionChangeEvent
import com.openlattice.authorization.AceKey
import com.openlattice.authorization.AceValue
import com.openlattice.authorization.Permission
import com.openlattice.authorization.PrincipalType
import com.openlattice.authorization.securable.SecurableObjectType

/**
 * Handles internal permission change events. This class sits far below of authorization layer, so should not be
 * responsible for handling audit related events.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PermissionMapListener(private val eventBus: EventBus) : EntryAddedListener<AceKey, AceValue>,
                                                              EntryRemovedListener<AceKey, AceValue>,
                                                              EntryUpdatedListener<AceKey, AceValue> {
    override fun entryAdded(event: EntryEvent<AceKey, AceValue>) {
        if (isMaterializationEvent(event)) {
            postMaterializationEvent(event)
        }
    }

    override fun entryRemoved(event: EntryEvent<AceKey, AceValue>) {
        if (isMaterializationEvent(event)) {
            postMaterializationEvent(event)
        }
    }

    override fun entryUpdated(event: EntryEvent<AceKey, AceValue>) {
        if (isMaterializationEvent(event)) {
            postMaterializationEvent(event)
        }
    }

    private fun postMaterializationEvent(event: EntryEvent<AceKey, AceValue>) {
        eventBus.post(
                MaterializePermissionChangeEvent(
                        event.key.principal,
                        setOf(event.key.aclKey.first()),
                        event.value.securableObjectType
                )
        )
    }

    private fun isMaterializationEvent(event: EntryEvent<AceKey, AceValue>): Boolean {
        return event.value.contains(Permission.MATERIALIZE) &&
                event.key.principal.type == PrincipalType.ORGANIZATION &&
                (event.value.securableObjectType == SecurableObjectType.EntitySet || event.value.securableObjectType == SecurableObjectType.PropertyTypeInEntitySet)
    }


    override fun equals(other: Any?): Boolean {
        return other != null && other is PermissionMapListener
    }

    override fun hashCode(): Int {
        return eventBus.hashCode()
    }


}