package com.openlattice.organizations

import com.openlattice.authorization.Principal
import java.util.*

class SortedPrincipalSet(principalSet: NavigableSet<Principal>) : NavigableSet<Principal> by principalSet