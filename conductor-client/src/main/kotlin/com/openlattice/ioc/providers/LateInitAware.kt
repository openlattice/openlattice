package com.openlattice.ioc.providers

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface LateInitAware {
    fun setLateInitProvider(lateInitProvider: LateInitProvider )
}