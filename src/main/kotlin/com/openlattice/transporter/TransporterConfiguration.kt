package com.openlattice.transporter

import com.zaxxer.hikari.HikariConfig
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class TransporterConfiguration(
        val server: Properties,
        val user: String,
        val password: String
)