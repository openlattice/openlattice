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
    REPLACE_PROPERTIES_OF_ENTITIES,
    PARTIAL_REPLACE_ENTITIES,
    MERGE_ENTITIES,
    CLEAR_ENTITIES,
    DELETE_ENTITIES,
    DELETE_PROPERTIES_OF_ENTITIES,
    DELETE_ENTITY_AND_NEIGHBORHOOD,
    DELETE_ENTITY_AS_PART_OF_NEIGHBORHOOD,
    ASSOCIATE_ENTITIES,

    ADD_PERMISSION,
    REMOVE_PERMISSON,

    CREATE_ASSOCIATIONS,

    SEARCH_ENTITY_SET_DATA,
    LOAD_ENTITY_NEIGHBORS,

    CREATE_ENTITY_SET,
    UPDATE_ENTITY_SET,
    READ_ENTITY_SET,
    CLEAR_ENTITY_SET,
    DELETE_ENTITY_SET,
    UPDATE_ENTITY_SET_PROPERTY_METADATA,

    CREATE_PROPERTY_TYPE,
    UPDATE_PROPERTY_TYPE,
    DELETE_PROPERTY_TYPE,
    CREATE_ENTITY_TYPE,
    UPDATE_ENTITY_TYPE,
    ADD_PROPERTY_TYPE_TO_ENTITY_TYPE,
    REMOVE_PROPERTY_TYPE_FROM_ENTITY_TYPE,
    DELETE_ENTITY_TYPE,
    CREATE_ASSOCIATION_TYPE,
    UPDATE_ASSOCIATION_TYPE,
    ADD_ENTITY_TYPE_TO_ASSOCIATION_TYPE,
    REMOVE_ENTITY_TYPE_FROM_ASSOCIATION_TYPE,
    DELETE_ASSOCIATION_TYPE
}