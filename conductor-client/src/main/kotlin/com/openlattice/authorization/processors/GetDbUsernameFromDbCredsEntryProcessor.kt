package com.openlattice.authorization.processors

import com.hazelcast.core.Offloadable
import com.hazelcast.spi.impl.executionservice.ExecutionService
import com.openlattice.authorization.AclKey
import com.openlattice.directory.MaterializedViewAccount
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import org.slf4j.LoggerFactory

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class GetDbUsernameFromDbCredsEntryProcessor: Offloadable,
        AbstractReadOnlyRhizomeEntryProcessor<AclKey, MaterializedViewAccount, String>() {

    companion object {
        private val logger = LoggerFactory.getLogger(GetDbUsernameFromDbCredsEntryProcessor::class.java)
    }

    override fun process(entry: MutableMap.MutableEntry<AclKey, MaterializedViewAccount?>): String {
        val account = entry.value
        if (account == null) {
            logger.error("encountered null database account - acl key {}", entry.key)
            return ""
        }
        if (account.username == null || account.username.isBlank()) {
            logger.error(
                "encountered null/blank database username (should be impossible) - acl key {} username {}",
                entry.key,
                account.username
            )
            return ""
        }
        return account.username
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }
}
