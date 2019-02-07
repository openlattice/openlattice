/*
 * Copyright (C) 2019. OpenLattice, Inc.
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

package com.openlattice.assembler

import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@ReloadableConfiguration(uri = "assembler.yaml")
data class AssemblerConfiguration(
        val server: Properties,
        val foreignHost: String,
        val foreignDbName: String,
        val foreignPort: Short,
        val foreignUsername: String,
        val foreignPassword: String,
        val ssl: Boolean = true,
        val initialize: Optional<Boolean> = Optional.empty(),
        val testEntitySet: Optional<UUID> = Optional.empty()
)