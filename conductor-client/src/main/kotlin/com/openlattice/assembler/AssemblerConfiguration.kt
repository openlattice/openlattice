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

import com.fasterxml.jackson.annotation.JsonProperty
import com.geekbeast.rhizome.configuration.Configuration
import com.geekbeast.rhizome.configuration.ConfigurationKey
import com.geekbeast.rhizome.configuration.SimpleConfigurationKey
import com.geekbeast.rhizome.configuration.configuration.annotation.ReloadableConfiguration
import java.util.*

private const val configFileName = "assembler.yaml"

/**
 * [server] is a remote server that data will be transported to and assembled upon
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@ReloadableConfiguration(uri= configFileName)
data class AssemblerConfiguration(
        @JsonProperty val server: Properties,
        @JsonProperty val ssl: Boolean = true
): Configuration {

    companion object {
        @JvmStatic
        @get:JvmName("key")
        val key = SimpleConfigurationKey(configFileName)
    }

    override fun getKey(): ConfigurationKey {
        return AssemblerConfiguration.key
    }

    override fun toString(): String {
        return "AssemblerConfiguration(server=$server, ssl=$ssl)"
    }
}