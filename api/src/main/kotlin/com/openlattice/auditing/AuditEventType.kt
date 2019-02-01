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

package com.openlattice.auditing

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
enum class AuditEventType {
    READ_ENTITIES,
    CREATE_ENTITIES,
    REPLACE_ENTITIES,
    PARTIAL_REPLACE_ENTITIES,
    MERGE_ENTITIES,
    CLEAR_ENTITIES,
    DELETE_ENTITIES,
    READ_ENTITY_SET,
    CLEAR_ENTITY_SET,
    DELETE_ENTITY_SET,
    ADD_PERMISSION,
    REMOVE_PERMISSON,

}