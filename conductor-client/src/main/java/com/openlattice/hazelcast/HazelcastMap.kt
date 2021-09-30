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
package com.openlattice.hazelcast

import com.auth0.json.mgmt.users.User
import com.geekbeast.rhizome.hazelcast.DelegatedIntList
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.openlattice.apps.App
import com.openlattice.apps.AppConfigKey
import com.openlattice.apps.AppTypeSetting
import com.openlattice.assembler.EntitySetAssemblyKey
import com.openlattice.assembler.MaterializedEntitySet
import com.openlattice.assembler.OrganizationAssembly
import com.openlattice.auditing.AuditRecordEntitySetConfiguration
import com.openlattice.authorization.AccessTarget
import com.openlattice.authorization.AceKey
import com.openlattice.authorization.AceValue
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AclKeySet
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.codex.Base64Media
import com.openlattice.collaborations.Collaboration
import com.openlattice.collaborations.ProjectedTableKey
import com.openlattice.collaborations.ProjectedTableMetadata
import com.openlattice.collections.CollectionTemplateKey
import com.openlattice.collections.EntitySetCollection
import com.openlattice.collections.EntityTypeCollection
import com.openlattice.data.EntityDataKey
import com.openlattice.datasets.SecurableObjectMetadata
import com.openlattice.directory.MaterializedViewAccount
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.AssociationType
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.EntityTypePropertyKey
import com.openlattice.edm.type.EntityTypePropertyMetadata
import com.openlattice.edm.type.PropertyType
import com.openlattice.ids.Range
import com.openlattice.linking.EntityKeyPair
import com.openlattice.notifications.sms.SmsEntitySetInformation
import com.openlattice.notifications.sms.SmsInformationKey
import com.openlattice.organization.ExternalColumn
import com.openlattice.organization.ExternalTable
import com.openlattice.organizations.Organization
import com.openlattice.organizations.OrganizationDatabase
import com.openlattice.organizations.SortedPrincipalSet
import com.openlattice.postgres.mapstores.TypedMapIdentifier
import com.openlattice.requests.Status
import com.openlattice.rhizome.DelegatedIntSet
import com.openlattice.rhizome.hazelcast.DelegatedStringSet
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet
import com.openlattice.scheduling.ScheduledTask
import com.openlattice.shuttle.Integration
import com.openlattice.shuttle.IntegrationJob
import com.openlattice.transporter.types.TransporterColumnSet
import com.openlattice.organizations.JdbcConnectionParameters
import java.util.*

class HazelcastMap<K, V> internal constructor(val name: String) : TypedMapIdentifier<K, V> {
    private val checker = instanceChecker.checkInstance(name)

    init {
        if (checker.ok()) {
            valuesCache.add(this)
        }
    }

    override fun name(): String {
        this.checker.check()
        return name
    }

    override fun toString(): String {
        return name
    }

    fun getMap(hazelcast: HazelcastInstance): IMap<K, V> {
        this.checker.check()
        return hazelcast.getMap(name)
    }

    companion object {
        private val valuesCache: MutableList<HazelcastMap<*, *>> = ArrayList()
        private val instanceChecker = UniqueInstanceManager(HazelcastMap::class.java)

        // @formatter:off

        // When adding new entries to this list, please make sure to keep it sorted and keep the name in sync

        // @formatter:off

        @JvmField val ACL_KEYS = HazelcastMap<String, UUID>("ACL_KEYS")
        @JvmField val ALLOWED_EMAIL_DOMAINS = HazelcastMap<UUID, DelegatedStringSet>("ALLOWED_EMAIL_DOMAINS")
        @JvmField val APP_CONFIGS = HazelcastMap<AppConfigKey, AppTypeSetting>("APP_CONFIGS")
        @JvmField val APPS = HazelcastMap<UUID, App>("APPS")
        @JvmField val ASSEMBLIES = HazelcastMap<UUID, OrganizationAssembly>("ASSEMBLIES")
        @JvmField val ASSOCIATION_TYPES = HazelcastMap<UUID, AssociationType>("ASSOCIATION_TYPES")
        @JvmField val AUDIT_RECORD_ENTITY_SETS = HazelcastMap<AclKey, AuditRecordEntitySetConfiguration>("AUDIT_RECORD_ENTITY_SETS")
        @JvmField val CODEX_LOCKS = HazelcastMap<SmsInformationKey, Long>("CODEX_LOCKS")
        @JvmField val CODEX_MEDIA = HazelcastMap<UUID, Base64Media>("CODEX_MEDIA")
        @JvmField val COLLABORATIONS = HazelcastMap<UUID, Collaboration>("COLLABORATIONS")
        @JvmField val DB_CREDS = HazelcastMap<AclKey, MaterializedViewAccount>("DB_CREDS")
        @JvmField val DELETED_ENTITY_SETS = HazelcastMap<UUID, DelegatedIntSet>("DELETED_ENTITY_SETS")
        @JvmField val ENTITY_SET_COLLECTION_CONFIG = HazelcastMap<CollectionTemplateKey, UUID>("ENTITY_SET_COLLECTION_CONFIG")
        @JvmField val ENTITY_SET_COLLECTIONS = HazelcastMap<UUID, EntitySetCollection>("ENTITY_SET_COLLECTIONS")
        @JvmField val ENTITY_SETS = HazelcastMap<UUID, EntitySet>("ENTITY_SETS")
        @JvmField val ENTITY_TYPE_COLLECTIONS = HazelcastMap<UUID, EntityTypeCollection>("ENTITY_TYPE_COLLECTIONS")
        @JvmField val ENTITY_TYPE_PROPERTY_METADATA = HazelcastMap<EntityTypePropertyKey, EntityTypePropertyMetadata>("ENTITY_TYPE_PROPERTY_METADATA")
        @JvmField val ENTITY_TYPES = HazelcastMap<UUID, EntityType>("ENTITY_TYPES")
        @JvmField val EXTERNAL_PERMISSION_ROLES = HazelcastMap<AccessTarget, Pair<String, UUID>>("EXTERNAL_PERMISSION_ROLES")
        @JvmField val EXTERNAL_COLUMNS = HazelcastMap<UUID, ExternalColumn>("EXTERNAL_COLUMNS")
        @JvmField val EXTERNAL_TABLES = HazelcastMap<UUID, ExternalTable>("EXTERNAL_TABLES")
        @JvmField val ID_GENERATION = HazelcastMap<Long, Range>("ID_GENERATION")
        @JvmField val INDEXING_JOBS = HazelcastMap<UUID, DelegatedUUIDSet>("INDEXING_JOBS")
        @JvmField val INDEXING_LOCKS = HazelcastMap<UUID, Long>("INDEXING_LOCKS")
        @JvmField val INDEXING_PROGRESS = HazelcastMap<UUID, UUID>("INDEXING_PROGRESS")
        @JvmField val INDEXING_PARTITION_LIST = HazelcastMap<UUID, DelegatedIntList>("INDEXING_PARTITION_LIST")
        @JvmField val INDEXING_PARTITION_PROGRESS = HazelcastMap<UUID, Int>("INDEXING_PARTITION_PROGRESS")
        @JvmField val INDEXING_GRAPH_PROCESSING = HazelcastMap<UUID, Long>("INDEXING_GRAPH_PROCESSING")
        @JvmField val INTEGRATION_JOBS = HazelcastMap<UUID, IntegrationJob>("INTEGRATION_JOBS")
        @JvmField val INTEGRATIONS = HazelcastMap<String, Integration>("INTEGRATIONS")
        @JvmField val LEGACY_PERMISSIONS = HazelcastMap<AceKey, AceValue>("LEGACY_PERMISSIONS") // needed for migration
        @JvmField val LINKING_FEEDBACK = HazelcastMap<EntityKeyPair, Boolean>("LINKING_FEEDBACK")
        @JvmField val LINKING_INDEXING_LOCKS = HazelcastMap<UUID, Long>("LINKING_INDEXING_LOCKS")
        @JvmField val LINKING_LOCKS = HazelcastMap<EntityDataKey, Long>("LINKING_LOCKS")
        @JvmField val LONG_IDS = HazelcastMap<String, Long>("LONG_IDS")
        @JvmField val MATERIALIZED_ENTITY_SETS = HazelcastMap<EntitySetAssemblyKey, MaterializedEntitySet>("MATERIALIZED_ENTITY_SETS")
        @JvmField val NAMES = HazelcastMap<UUID, String>("NAMES")
        @JvmField val OBJECT_METADATA = HazelcastMap<AclKey, SecurableObjectMetadata>("OBJECT_METADATA")
        @JvmField val ORGANIZATION_DATABASES = HazelcastMap<UUID, OrganizationDatabase>("ORGANIZATION_DATABASES")
        @JvmField val ORGANIZATIONS = HazelcastMap<UUID, Organization>("ORGANIZATIONS")
        @JvmField val PERMISSIONS = HazelcastMap<AceKey, AceValue>("PERMISSIONS")
        @JvmField val PRINCIPAL_TREES = HazelcastMap<AclKey, AclKeySet>("PRINCIPAL_TREES")
        @JvmField val PRINCIPALS = HazelcastMap<AclKey, SecurablePrincipal>("PRINCIPALS")
        @JvmField val PROJECTED_TABLES = HazelcastMap<ProjectedTableKey, ProjectedTableMetadata>("PROJECTED_TABLES")
        @JvmField val PROPERTY_TYPES = HazelcastMap<UUID, PropertyType>("PROPERTY_TYPES")
        @JvmField val REQUESTS = HazelcastMap<AceKey, Status>("REQUESTS")
        @JvmField val RESOLVED_PRINCIPAL_TREES = HazelcastMap<String, SortedPrincipalSet>("RESOLVED_PRINCIPAL_TREES")
        @JvmField val SCHEDULED_TASK_LOCKS = HazelcastMap<UUID, Long>("SCHEDULED_TASK_LOCKS")
        @JvmField val SCHEDULED_TASKS = HazelcastMap<UUID, ScheduledTask>("SCHEDULED_TASKS")
        @JvmField val SCHEMAS = HazelcastMap<String, DelegatedStringSet>("SCHEMAS")
        @JvmField val SECURABLE_OBJECT_TYPES = HazelcastMap<AclKey, SecurableObjectType>("SECURABLE_OBJECT_TYPES")
        @JvmField val SECURABLE_PRINCIPALS = HazelcastMap<String, SecurablePrincipal>("SECURABLE_PRINCIPALS")
        @JvmField val SMS_INFORMATION = HazelcastMap<SmsInformationKey, SmsEntitySetInformation>("SMS_INFORMATION")
        @JvmField val TRANSPORTER_DB_COLUMNS = HazelcastMap<UUID, TransporterColumnSet>("TRANSPORTER_DB_COLUMNS")
        @JvmField val USERS = HazelcastMap<String, User>("USERS")
        @JvmField val WAREHOUSES = HazelcastMap<UUID, JdbcConnectionParameters>("WAREHOUSES")

        // @formatter:on

        @JvmStatic
        fun values(): Array<HazelcastMap<*, *>> {
            return valuesCache.toTypedArray()
        }

        @JvmStatic
        fun valueOf(name: String): HazelcastMap<*, *> {
            for (e in valuesCache) {
                if (e.name == name) {
                    return e
                }
            }
            throw IllegalArgumentException("Map with name \"$name\" not found")
        }
    }
}