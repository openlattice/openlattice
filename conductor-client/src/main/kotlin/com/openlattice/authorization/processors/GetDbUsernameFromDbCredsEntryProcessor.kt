package com.openlattice.authorization.processors

import com.hazelcast.core.Offloadable
import com.hazelcast.spi.impl.executionservice.ExecutionService
import com.openlattice.authorization.AclKey
import com.openlattice.directory.MaterializedViewAccount
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class GetDbUsernameFromDbCredsEntryProcessor: Offloadable,
        AbstractReadOnlyRhizomeEntryProcessor<AclKey, MaterializedViewAccount, String>() {

    override fun process(entry: MutableMap.MutableEntry<AclKey, MaterializedViewAccount>): String {
        return entry.value.username
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }
}