package com.openlattice.authorization

import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
data class AccessTarget(val aclKey: AclKey, val permission: Permission) {
    companion object {
        @JvmStatic
        fun forPermissionOnTarget( permission: Permission, root: UUID, vararg targetParts: UUID): AccessTarget {
            return AccessTarget( AclKey(root, *targetParts), permission)
        }
    }
}