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

import com.google.common.base.Preconditions.checkArgument

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresDatabases private constructor() {
    companion object {
        private const val MIN_LENGTH = 8
        private const val MAX_LENGTH = 50
        private val DB_NAME_CHARS = Regex("[a-zA-Z0-9_\\-]*")

        @JvmStatic
        fun assertDatabaseNameIsValid(name: String) {
            checkArgument(name.length in MIN_LENGTH..MAX_LENGTH,
                    "Database name $name is invalid: names must be between " +
                            "$MIN_LENGTH and $MAX_LENGTH characters, inclusive."
            )

            checkArgument(DB_NAME_CHARS.containsMatchIn(name), "Database name $name contains " +
                    "invalid characters. Names can contain only alphanumeric characters, dashes, and underscores.")

        }
    }
}