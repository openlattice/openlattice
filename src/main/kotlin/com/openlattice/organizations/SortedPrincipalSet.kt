package com.openlattice.organizations

import com.openlattice.authorization.Principal
import java.util.*

data class SortedPrincipalSet(val principalSet: NavigableSet<Principal>) : NavigableSet<Principal> by principalSet