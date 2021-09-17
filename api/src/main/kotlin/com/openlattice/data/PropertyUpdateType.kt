package com.openlattice.data

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
enum class PropertyUpdateType {
    /**
     * Indicates that an additional version should be recorded in the versions array.
     */
    Versioned,
    /**
     * Indicates that an additional version should not be recorded in the versions array.
     */
    Unversioned
}