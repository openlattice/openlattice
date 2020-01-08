

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

import com.auth0.json.mgmt.users.User;
import com.geekbeast.rhizome.hazelcast.DelegatedIntList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.apps.App;
import com.openlattice.apps.AppConfigKey;
import com.openlattice.apps.AppType;
import com.openlattice.apps.AppTypeSetting;
import com.openlattice.assembler.EntitySetAssemblyKey;
import com.openlattice.assembler.MaterializedEntitySet;
import com.openlattice.assembler.OrganizationAssembly;
import com.openlattice.auditing.AuditRecordEntitySetConfiguration;
import com.openlattice.authorization.*;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.collections.CollectionTemplateKey;
import com.openlattice.collections.EntitySetCollection;
import com.openlattice.collections.EntityTypeCollection;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityKey;
import com.openlattice.data.TicketKey;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.set.EntitySetPropertyKey;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.edm.type.*;
import com.openlattice.ids.Range;
import com.openlattice.linking.EntityKeyPair;
import com.openlattice.notifications.sms.SmsEntitySetInformation;
import com.openlattice.notifications.sms.SmsInformationKey;
import com.openlattice.organization.OrganizationExternalDatabaseColumn;
import com.openlattice.organization.OrganizationExternalDatabaseTable;
import com.openlattice.organizations.Organization;
import com.openlattice.organizations.PrincipalSet;
import com.openlattice.organizations.SortedPrincipalSet;
import com.openlattice.postgres.PostgresAuthenticationRecord;
import com.openlattice.postgres.mapstores.TypedMapIdentifier;
import com.openlattice.requests.Status;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class HazelcastMap<K, V> implements TypedMapIdentifier<K, V> {
    private static final List<HazelcastMap<?,?>> valuesCache = new ArrayList<>();
    public final String name;

    private HazelcastMap(String name) {
        this.name = name;
        valuesCache.add(this);
    }
    public String name() {
        return this.name;
    }

    public static HazelcastMap<?, ?>[] values() {
        return valuesCache.toArray(new HazelcastMap[valuesCache.size()]);
    }
    public static HazelcastMap<?, ?> valueOf(String name) {
        for (HazelcastMap<?, ?> e : valuesCache) {
            if (e.name == name) {
                return e;
            }
        }
        throw new IllegalArgumentException(name);
    }

    @Override
    public String toString() {
        return name;
    }

    public IMap<K, V> getMap(HazelcastInstance hazelcast) {
        return hazelcast.getMap(name);
    }

    public static final HazelcastMap<String, UUID> ACL_KEYS = new HazelcastMap<>("ACL_KEYS");
    public static final HazelcastMap<UUID, App> APPS = new HazelcastMap<>("APPS");
    public static final HazelcastMap<UUID, AppType> APP_TYPES = new HazelcastMap<>("APP_TYPES");
    public static final HazelcastMap<AppConfigKey, AppTypeSetting> APP_CONFIGS = new HazelcastMap<>("APP_CONFIGS");
    public static final HazelcastMap<UUID, OrganizationAssembly> ASSEMBLIES = new HazelcastMap<>("ASSEMBLIES");
    public static final HazelcastMap<UUID, AssociationType> ASSOCIATION_TYPES = new HazelcastMap<>("ASSOCIATION_TYPES");
    // public static final HazelcastMap<Object, Object> AUDIT_LOG = new HazelcastMap<>("AUDIT_LOG");
    // public static final HazelcastMap<Object, Object> AUDIT_METRICS = new HazelcastMap<>("AUDIT_METRICS");
    public static final HazelcastMap<AclKey, AuditRecordEntitySetConfiguration> AUDIT_RECORD_ENTITY_SETS = new HazelcastMap<>("AUDIT_RECORD_ENTITY_SETS");
    // public static final HazelcastMap<Object, Object> BACKEDGES = new HazelcastMap<>("BACKEDGES");
    // public static final HazelcastMap<Object, Object> COMPLEX_TYPES = new HazelcastMap<>("COMPLEX_TYPES");
    // public static final HazelcastMap<Object, Object> DATA = new HazelcastMap<>("DATA");
    public static final HazelcastMap<String, String> DB_CREDS = new HazelcastMap<>("DB_CREDS");
    public static final HazelcastMap<UUID, Long> DELETION_LOCKS = new HazelcastMap<>("DELETION_LOCKS");
    // public static final HazelcastMap<Object, Object> EDGES = new HazelcastMap<>("EDGES");
    // public static final HazelcastMap<Object, Object> ENTITY_DATA = new HazelcastMap<>("ENTITY_DATA");
    // public static final HazelcastMap<Object, Object> ENTITY_LINKING_LOCKS = new HazelcastMap<>("ENTITY_LINKING_LOCKS");
    public static final HazelcastMap<CollectionTemplateKey, UUID> ENTITY_SET_COLLECTION_CONFIG = new HazelcastMap<>("ENTITY_SET_COLLECTION_CONFIG");
    public static final HazelcastMap<UUID, EntitySetCollection> ENTITY_SET_COLLECTIONS = new HazelcastMap<>("ENTITY_SET_COLLECTIONS");
    // public static final HazelcastMap<Object, Object> ENTITY_SET_COUNTERS = new HazelcastMap<>("ENTITY_SET_COUNTERS");
    public static final HazelcastMap<TicketKey, DelegatedUUIDSet> ENTITY_SET_PROPERTIES_TICKETS = new HazelcastMap<>("ENTITY_SET_PROPERTIES_TICKETS");
    public static final HazelcastMap<EntitySetPropertyKey, EntitySetPropertyMetadata> ENTITY_SET_PROPERTY_METADATA = new HazelcastMap<>("ENTITY_SET_PROPERTY_METADATA");
    public static final HazelcastMap<TicketKey, UUID> ENTITY_SET_TICKETS = new HazelcastMap<>("ENTITY_SET_TICKETS");
    public static final HazelcastMap<UUID, EntitySet> ENTITY_SETS = new HazelcastMap<>("ENTITY_SETS");
    public static final HazelcastMap<UUID, EntityTypeCollection> ENTITY_TYPE_COLLECTIONS = new HazelcastMap<>("ENTITY_TYPE_COLLECTIONS");
    public static final HazelcastMap<EntityTypePropertyKey, EntityTypePropertyMetadata> ENTITY_TYPE_PROPERTY_METADATA = new HazelcastMap<>("ENTITY_TYPE_PROPERTY_METADATA");
    public static final HazelcastMap<UUID, EntityType> ENTITY_TYPES = new HazelcastMap<>("ENTITY_TYPES");
    public static final HazelcastMap<UUID, Long> EXPIRATION_LOCKS = new HazelcastMap<>("EXPIRATION_LOCKS");
    public static final HazelcastMap<String, PostgresAuthenticationRecord> HBA_AUTHENTICATION_RECORDS = new HazelcastMap<>("HBA_AUTHENTICATION_RECORDS");
    // public static final HazelcastMap<Object, Object> IDS = new HazelcastMap<>("IDS");
    public static final HazelcastMap<Long, Range> ID_GENERATION = new HazelcastMap<>("ID_GENERATION");
    public static final HazelcastMap<EntityKey, Long> ID_REF_COUNTS = new HazelcastMap<>("ID_REF_COUNTS");
    public static final HazelcastMap<EntityKey, UUID> ID_CACHE = new HazelcastMap<>("ID_CACHE");
    public static final HazelcastMap<UUID, DelegatedUUIDSet> INDEXING_JOBS = new HazelcastMap<>("INDEXING_JOBS");
    public static final HazelcastMap<UUID, Long> INDEXING_LOCKS = new HazelcastMap<>("INDEXING_LOCKS");
    public static final HazelcastMap<UUID, UUID> INDEXING_PROGRESS = new HazelcastMap<>("INDEXING_PROGRESS");
    public static final HazelcastMap<UUID, Integer> INDEXING_PARTITION_PROGRESS = new HazelcastMap<>("INDEXING_PARTITION_PROGRESS");
    public static final HazelcastMap<UUID, DelegatedIntList> INDEXING_PARTITION_LIST = new HazelcastMap<>("INDEXING_PARTITION_LIST");
    public static final HazelcastMap<UUID, Long> INDEXING_GRAPH_PROCESSING = new HazelcastMap<>("INDEXING_GRAPH_PROCESSING");
    // public static final HazelcastMap<Object, Object> KEYS = new HazelcastMap<>("KEYS");
    // public static final HazelcastMap<Object, Object> LINKED_ENTITY_TYPES = new HazelcastMap<>("LINKED_ENTITY_TYPES");
    // public static final HazelcastMap<Object, Object> LINKED_ENTITY_SETS = new HazelcastMap<>("LINKED_ENTITY_SETS");
    // public static final HazelcastMap<Object, Object> LINKING_CURSORS = new HazelcastMap<>("LINKING_CURSORS");
    // public static final HazelcastMap<Object, Object> LINKING_EDGES = new HazelcastMap<>("LINKING_EDGES");
    // public static final HazelcastMap<Object, Object> LINKING_ENTITIES = new HazelcastMap<>("LINKING_ENTITIES");
    // public static final HazelcastMap<Object, Object> LINKING_ENTITY_VERTICES = new HazelcastMap<>("LINKING_ENTITY_VERTICES");
    public static final HazelcastMap<EntityKeyPair, Boolean> LINKING_FEEDBACK = new HazelcastMap<>("LINKING_FEEDBACK");
    public static final HazelcastMap<UUID, Long> LINKING_INDEXING_LOCKS = new HazelcastMap<>("LINKING_INDEXING_LOCKS");
    public static final HazelcastMap<EntityDataKey, Long> LINKING_LOCKS = new HazelcastMap<>("LINKING_LOCKS");
    // public static final HazelcastMap<Object, Object> LINKING_VERTICES = new HazelcastMap<>("LINKING_VERTICES");
    public static final HazelcastMap<String, Long> LONG_IDS = new HazelcastMap<>("LONG_IDS");
    public static final HazelcastMap<EntitySetAssemblyKey, MaterializedEntitySet> MATERIALIZED_ENTITY_SETS = new HazelcastMap<>("MATERIALIZED_ENTITY_SETS");
    public static final HazelcastMap<AceKey, AceValue> PERMISSIONS = new HazelcastMap<>("PERMISSIONS");
    // public static final HazelcastMap<Object, Object> PERMISSIONS_REQUESTS_UNRESOLVED = new HazelcastMap<>("PERMISSIONS_REQUESTS_UNRESOLVED");
    // public static final HazelcastMap<Object, Object> PERMISSIONS_REQUESTS_RESOLVED = new HazelcastMap<>("PERMISSIONS_REQUESTS_RESOLVED");
    public static final HazelcastMap<AclKey, AclKeySet> PRINCIPAL_TREES = new HazelcastMap<>("PRINCIPAL_TREES");
    public static final HazelcastMap<UUID, PropertyType> PROPERTY_TYPES = new HazelcastMap<>("PROPERTY_TYPES");
    public static final HazelcastMap<UUID, DelegatedUUIDSet> ORGANIZATION_APPS = new HazelcastMap<>("ORGANIZATION_APPS");
    public static final HazelcastMap<UUID, Organization> ORGANIZATIONS = new HazelcastMap<>("ORGANIZATIONS");
    public static final HazelcastMap<UUID, String> NAMES = new HazelcastMap<>("NAMES");
    public static final HazelcastMap<String, DelegatedStringSet> SCHEMAS = new HazelcastMap<>("SCHEMAS");
    // @Deprecated public static final HazelcastMap<Object, Object> VISIBILITY = new HazelcastMap<>("VISIBILITY");
    // public static final HazelcastMap<Object, Object> ORGANIZATIONS_TITLES = new HazelcastMap<>("ORGANIZATIONS_TITLES");
    public static final HazelcastMap<UUID, String> ORGANIZATIONS_DESCRIPTIONS = new HazelcastMap<>("ORGANIZATIONS_DESCRIPTIONS");
    public static final HazelcastMap<UUID, DelegatedStringSet> ALLOWED_EMAIL_DOMAINS = new HazelcastMap<>("ALLOWED_EMAIL_DOMAINS");
    public static final HazelcastMap<UUID, PrincipalSet> ORGANIZATIONS_MEMBERS = new HazelcastMap<>("ORGANIZATIONS_MEMBERS");
    public static final HazelcastMap<SmsInformationKey, SmsEntitySetInformation> SMS_INFORMATION = new HazelcastMap<>("SMS_INFORMATION");
    public static final HazelcastMap<AclKey, SecurablePrincipal> PRINCIPALS = new HazelcastMap<>("PRINCIPALS");
    // public static final HazelcastMap<Object, Object> ORGANIZATIONS_ROLES = new HazelcastMap<>("ORGANIZATIONS_ROLES");
    public static final HazelcastMap<AceKey, Status> REQUESTS = new HazelcastMap<>("REQUESTS");
    public static final HazelcastMap<AclKey, SecurableObjectType> SECURABLE_OBJECT_TYPES = new HazelcastMap<>("SECURABLE_OBJECT_TYPES");
    // public static final HazelcastMap<Object, Object> TOKEN_ACCEPTANCE_TIME = new HazelcastMap<>("TOKEN_ACCEPTANCE_TIME");
    public static final HazelcastMap<String, User> USERS = new HazelcastMap<>("USERS");
    // public static final HazelcastMap<Object, Object> VERTICES = new HazelcastMap<>("VERTICES");
    // public static final HazelcastMap<Object, Object> VERTEX_IDS_AFTER_LINKING = new HazelcastMap<>("VERTEX_IDS_AFTER_LINKING");
    // public static final HazelcastMap<Object, Object> PERSISTENT_SEARCHES = new HazelcastMap<>("PERSISTENT_SEARCHES");
    // public static final HazelcastMap<Object, Object> ENTITY_WRITE_LOCKS = new HazelcastMap<>("ENTITY_WRITE_LOCKS");
    public static final HazelcastMap<UUID, OrganizationExternalDatabaseColumn> ORGANIZATION_EXTERNAL_DATABASE_COLUMN = new HazelcastMap<>("ORGANIZATION_EXTERNAL_DATABASE_COLUMN");
    public static final HazelcastMap<UUID, OrganizationExternalDatabaseTable> ORGANIZATION_EXTERNAL_DATABASE_TABLE = new HazelcastMap<>("ORGANIZATION_EXTERNAL_DATABASE_TABLE");
    // public static final HazelcastMap<Object, Object> TRIGGERS = new HazelcastMap<>("TRIGGERS");
    public static final HazelcastMap<String, SecurablePrincipal> SECURABLE_PRINCIPALS = new HazelcastMap<>("SECURABLE_PRINCIPALS");
    public static final HazelcastMap<String, SortedPrincipalSet> RESOLVED_PRINCIPAL_TREES = new HazelcastMap<>("RESOLVED_PRINCIPAL_TREES");
}


