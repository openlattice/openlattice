/*
 * Copyright (C) 2020. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.transporter

import com.kryptnostic.rhizome.configuration.RhizomeConfiguration
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.transporter.types.TransporterDatastore
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.io.IOException
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
class TransporterConfigurationPod {
    @Inject
    private lateinit var assemblerConfiguration: AssemblerConfiguration

    @Inject
    private lateinit var externalDbConnMan: ExternalDatabaseConnectionManager

    @Inject
    private lateinit var rhizome: RhizomeConfiguration

    @Bean(name = ["transporterDatastore"])
    @Throws(IOException::class)
    fun transporterDatastore(): TransporterDatastore {
        LoggerFactory.getLogger(TransporterConfigurationPod::class.java).info("Constructing TransporterDatastore")
        return TransporterDatastore(assemblerConfiguration, rhizome, externalDbConnMan)
    }
}
