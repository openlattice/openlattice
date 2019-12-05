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

package com.openlattice.tasks

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
enum class Task {
    AUDIT_INITIALIZATION,
    AUTH0_SYNC_INITIALIZATION_TASK,
    AUTH0_SYNC_TASK,
    AUTHORIZATION_BOOTSTRAP,
    CLEAN_OUT_OLDER_USERS_INITIALIZATON,
    ENTITY_VIEWS_INITIALIZER,
    EDM_SYNC_INITIALIZER,
    ID_CONSTANT_RESERVATION,
    ID_GEN_CATCH_UP,
    MATERIALIZED_ENTITY_SETS_DATA_REFRESH_TASK,
    ORGANIZATION_ASSEMBLIES_INITIALIZER,
    ORGANIZATION_INITIALIZATION,
    ORGANIZATION_MEMBERS_CLEANUP,
    PERSISTENT_SEARCH_MESSENGER_TASK,
    PRODUCTION_VIEW_INITIALIZATON,
    POST_INITIALIZER,
    POSTGRES_ENTITY_SET_SIZES_INITIALIZATION,
    POSTGRES_ENTITY_SET_SIZES_REFRESH_TASK,
    POSTGRES_META_DATA_PROPERTIES_INITIALIZATION,
    USERS_AND_ROLES_INITIALIZATON,
    USER_CREDENTIAL_SYNC_TASK,
    MATERIALIZE_PERMISSION_SYNC_TASK,
    EXTERNAL_DATABASE_PERMISSIONS_SYNC_TASK
}