package com.openlattice.postgres.external

/**
 * @author Drew Bailey (drew@openlattice.com)
 *
 * Schemas:
 * - "entitysets"
 *      schema in org_* database where views are assembled
 * - "transporter"
 *      schema in org_* database where foreign et tables are accessible
 * - "ol"
 *      schema in atlas transporter database where production tables are accessible
 */
enum class Schemas(val label: String) {
    INTEGRATIONS_SCHEMA("integrations"),
    OPENLATTICE_SCHEMA("openlattice"),
    PUBLIC_SCHEMA("public"),
    TRANSPORTER_SCHEMA("transporter"),
    ASSEMBLED_ENTITY_SETS("entitysets"),
    ENTERPRISE_FDW_SCHEMA("ol"),
    STAGING_SCHEMA("staging"),
    PROJECTIONS_SCHEMA("projections");

    override fun toString(): String {
        return label
    }

    companion object {

        @JvmStatic
        fun fromName(label: String): Schemas {
            for (e in values()) {
                if (e.label == label) {
                    return e
                }
            }
            throw IllegalArgumentException("Schema with name $label not found")
        }
    }
}
