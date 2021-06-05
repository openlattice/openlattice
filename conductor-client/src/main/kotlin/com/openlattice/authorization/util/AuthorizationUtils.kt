package com.openlattice.authorization.util

import com.openlattice.authorization.AceKey
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Principal
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
fun getLastAclKeySafely(aclKeys: List<UUID>): UUID? = aclKeys.lastOrNull()

fun toAceKeys(aclKeys: Set<AclKey>, principal: Principal): Set<AceKey> = aclKeys.mapTo(mutableSetOf()) {
    AceKey(it, principal)
}