package com.openlattice.hazelcast.processors

import com.hazelcast.core.Offloadable
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Principal
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class GetPrincipalFromSecurablePrincipalsEntryProcessor:
        AbstractReadOnlyRhizomeEntryProcessor<AclKey, SecurablePrincipal, Principal>(),
        Offloadable
{
    override fun process(entry: MutableMap.MutableEntry<AclKey, SecurablePrincipal>): Principal {
        return entry.value.principal
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }
}