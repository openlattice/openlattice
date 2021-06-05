package com.openlattice.principals

import com.hazelcast.core.Offloadable
import com.hazelcast.spi.impl.executionservice.ExecutionService
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor

class PrincipalExistsEntryProcessor :  AbstractReadOnlyRhizomeEntryProcessor<AclKey, SecurablePrincipal?, Boolean>(), Offloadable {

    override fun process(entry: MutableMap.MutableEntry<AclKey, SecurablePrincipal?>): Boolean {
        return entry.value != null
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }
}