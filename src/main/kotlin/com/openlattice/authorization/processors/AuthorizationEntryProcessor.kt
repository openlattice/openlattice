package com.openlattice.authorization.processors

import com.openlattice.authorization.AceKey
import com.openlattice.authorization.AceValue
import com.openlattice.authorization.DelegatedPermissionEnumSet
import com.openlattice.authorization.Permission
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

class AuthorizationEntryProcessor : AbstractReadOnlyRhizomeEntryProcessor<AceKey, AceValue, DelegatedPermissionEnumSet>() {

    override fun process(entry: MutableMap.MutableEntry<AceKey, AceValue?>): DelegatedPermissionEnumSet {
        return DelegatedPermissionEnumSet.wrap(entry.value?.permissions ?: EnumSet.noneOf(Permission::class.java))
    }
}