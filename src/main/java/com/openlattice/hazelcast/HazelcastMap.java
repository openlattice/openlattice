

/*
 * Copyright (C) 2018. OpenLattice, Inc.
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
 */

package com.openlattice.hazelcast;

import com.openlattice.conductor.codecs.odata.Table;

public enum HazelcastMap {
    ACL_KEYS( Table.ACL_KEYS ),
    ASSEMBLIES( null ),
    AUDIT_LOG( Table.AUDIT_LOG ),
    AUDIT_METRICS( Table.AUDIT_METRICS ),
    AUDIT_RECORD_ENTITY_SETS( Table.AUDIT_RECORD_ENTITY_SETS ),
    COMPLEX_TYPES( Table.COMPLEX_TYPES ),
    DATA( Table.DATA ),
    DB_CREDS( null ),
    EDM_VERSIONS( Table.EDM_VERSIONS ),
    ENUM_TYPES( Table.ENUM_TYPES ),
    ENTITY_DATA( null ),
    ENTITY_SET_COUNTERS( null ),
    IDS( Table.IDS ),
    ID_GENERATION( null ),
    INDEXING_LOCKS( null ),
    INDEXING_GRAPH_PROCESSING( null ),
    KEYS( Table.KEYS ),
    LINKED_ENTITY_TYPES( Table.LINKED_ENTITY_TYPES ),
    LINKED_ENTITY_SETS( Table.LINKED_ENTITY_SETS ),
    LINKING_EDGES( Table.WEIGHTED_LINKING_EDGES ),
    LINKING_ENTITY_VERTICES( Table.LINKING_ENTITY_VERTICES ),
    LINKING_VERTICES( Table.LINKING_VERTICES ),
    PERMISSIONS( Table.PERMISSIONS ),
    PERMISSIONS_REQUESTS_UNRESOLVED( Table.PERMISSIONS_REQUESTS_UNRESOLVED ),
    PERMISSIONS_REQUESTS_RESOLVED( Table.PERMISSIONS_REQUESTS_RESOLVED ),
    PRINCIPAL_TREES( null ),
    PROPERTY_TYPES( Table.PROPERTY_TYPES ),
    ENTITY_TYPES( Table.ENTITY_TYPES ),
    ENTITY_SETS( Table.ENTITY_SETS ),
    NAMES( Table.NAMES ),
    SCHEMAS( Table.SCHEMAS ),
    VISIBILITY( Table.ORGANIZATIONS ),
    ORGANIZATIONS_TITLES( Table.ORGANIZATIONS ),
    ORGANIZATIONS_DESCRIPTIONS( Table.ORGANIZATIONS ),
    ALLOWED_EMAIL_DOMAINS( Table.ORGANIZATIONS ),
    ORGANIZATIONS_MEMBERS( Table.ORGANIZATIONS ),
    PRINCIPALS( Table.ROLES ),
    ORGANIZATIONS_ROLES( Table.ORGANIZATIONS_ROLES ),
    USERS( null ),
    ENTITY_SET_TICKETS( null ),
    ENTITY_SET_PROPERTIES_TICKETS( null ),
    REQUESTS( Table.REQUESTS ),
    SECURABLE_OBJECT_TYPES( Table.PERMISSIONS ),
    ASSOCIATION_TYPES( Table.ASSOCIATION_TYPES ),
    TOKEN_ACCEPTANCE_TIME( null ),
    EDGES( Table.EDGES ),
    BACKEDGES( Table.BACK_EDGES ),
    VERTICES( Table.VERTICES ),
    SYNC_IDS( Table.SYNC_IDS ),
    VERTEX_IDS_AFTER_LINKING( Table.VERTEX_IDS_AFTER_LINKING ),
    ENTITY_SET_PROPERTY_METADATA( Table.ENTITY_SET_PROPERTY_METADATA ),
    LINKING_ENTITIES( Table.LINKING_ENTITIES ),
    APPS( Table.APPS ),
    APP_TYPES( Table.APP_TYPES ),
    APP_CONFIGS( Table.APP_CONFIGS ),
    ORGANIZATION_APPS( Table.ORGANIZATIONS ),
    PERSISTENT_SEARCHES( Table.PERSISTENT_SEARCHES );

    private final Table table;

    private HazelcastMap( Table table ) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

}
