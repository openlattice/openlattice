package com.openlattice.postgres.external

/**
 * @author Andrew Carter andrew@openlattice.com
 */
enum class Extensions(val label: String) {
    PGAUDIT_EXTENSION("pgaudit");

    override fun toString(): String {
        return label
    }

    companion object {

        @JvmStatic
        fun fromName(label: String): Schemas {
            for (e in Schemas.values()) {
                if (e.label == label) {
                    return e
                }
            }
            throw IllegalArgumentException("Extension with name $label not found")
        }
    }
}