package com.openlattice.postgres.external

/**
 * @author Drew Bailey (drewbaileym@gmail.com)
 */
enum class Schemas(val label: String) {
    INTEGRATIONS_SCHEMA("integrations"),
    OPENLATTICE_SCHEMA("openlattice"),
    TRANSPORTED_VIEWS_SCHEMA("ol"),
    PUBLIC_SCHEMA("public"),
    TRANSPORTER_SCHEMA("transporter"),
    ASSEMBLED_ENTITY_SETS("entitysets"),
    ENTERPRISE_FDW_SCHEMA("ol"),
    STAGING_SCHEMA("staging");

    override fun toString(): String {
        return label
    }
}
