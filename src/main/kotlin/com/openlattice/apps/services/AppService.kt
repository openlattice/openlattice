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
 */

package com.openlattice.apps.services

import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Maps
import com.google.common.collect.Sets
import com.google.common.eventbus.EventBus
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.hazelcast.query.Predicates
import com.openlattice.apps.*
import com.openlattice.apps.historical.HistoricalAppConfig
import com.openlattice.apps.historical.HistoricalAppTypeSetting
import com.openlattice.apps.processors.*
import com.openlattice.authorization.*
import com.openlattice.collections.CollectionsManager
import com.openlattice.collections.EntitySetCollection
import com.openlattice.collections.EntityTypeCollection
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.datastore.util.Util
import com.openlattice.edm.events.AppCreatedEvent
import com.openlattice.edm.events.AppDeletedEvent
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.mapstores.AppConfigMapstore
import java.util.*
import javax.inject.Inject

class AppService(
        hazelcast: HazelcastInstance,
        private val edmService: EdmManager,
        private val organizationService: HazelcastOrganizationService,
        private val authorizationService: AuthorizationManager,
        private val principalsService: SecurePrincipalsManager,
        private val reservations: HazelcastAclKeyReservationService,
        private val collectionsManager: CollectionsManager,
        private val entitySetService: EntitySetManager
) {
    private val apps: IMap<UUID, App> = hazelcast.getMap(HazelcastMap.APPS.name)
    private val appConfigs: IMap<AppConfigKey, AppTypeSetting> = hazelcast.getMap(HazelcastMap.APP_CONFIGS.name)
    private val aclKeys: IMap<String, UUID> = hazelcast.getMap(HazelcastMap.ACL_KEYS.name)
    private val entityTypeCollections: IMap<UUID, EntityTypeCollection> = hazelcast.getMap(HazelcastMap.ENTITY_TYPE_COLLECTIONS.name)
    private val entitySetCollections: IMap<UUID, EntitySetCollection> = hazelcast.getMap(HazelcastMap.ENTITY_SET_COLLECTIONS.name)

    @Inject
    private lateinit var eventBus: EventBus

    companion object {
        fun getAppPrincipalId(appId: UUID, organizationId: UUID): String? {
            return "$appId|$organizationId"
        }
    }

    fun getApps(): Iterable<App> {
        return apps.values
    }

    fun createApp(app: App): UUID {
        validateAppAndRoles(app)
        reservations.reserveIdAndValidateType(app, { app.name })
        apps[app.id] = app
        eventBus.post(AppCreatedEvent(app))
        return app.id
    }

    fun deleteApp(appId: UUID) {
        appConfigs.keySet(Predicates.equal(AppConfigMapstore.APP_ID, appId))
                .forEach { (appId1, organizationId) -> uninstallApp(appId1, organizationId) }
        apps.delete(appId)
        reservations.release(appId)
        eventBus.post(AppDeletedEvent(appId))
    }

    fun getApp(appId: UUID): App {
        val app = apps[appId]
        Preconditions.checkNotNull(app, "App with id $appId does not exist.")
        return app!!
    }

    fun getApp(name: String): App {
        val id = Util.getSafely(aclKeys, name)
        return getApp(id)
    }

    fun createNewAppRole(appId: UUID, role: AppRole): UUID? {
        validateAppAndRoles(appId, role)
        apps.executeOnKey(appId, AddRoleToAppProcessor(role))
        return role.id
    }

    fun deleteRoleFromApp(appId: UUID, roleId: UUID) {
        val app = getApp(appId)
        appConfigs.executeOnKeys(getAppConfigKeysForApp(appId), RemoveRoleFromAppConfigProcessor(roleId))
        apps.executeOnKey(appId, RemoveRoleFromAppProcessor(roleId))
    }

    private fun getAppConfigKeysForApp(appId: UUID): Set<AppConfigKey> {
        return appConfigs.keySet(Predicates.equal(AppConfigMapstore.APP_ID, appId))
    }

    private fun createRolesForApp(
            appRoles: Set<AppRole>,
            organizationId: UUID,
            entitySetCollectionId: UUID,
            appPrincipal: Principal,
            userPrincipal: Principal): MutableMap<UUID, AclKey> {
        val permissionsToGrant = Maps.newHashMap<AceKey, EnumSet<Permission>>()

        val entitySetCollection = collectionsManager.getEntitySetCollection(entitySetCollectionId)
        val entityTypeCollection = collectionsManager
                .getEntityTypeCollection(entitySetCollection.entityTypeCollectionId)
        val entityTypesById = edmService.getEntityTypesAsMap(entityTypeCollection.template.map { it.entityTypeId }.toSet())
        val entitySetsById = entitySetService
                .getEntitySetsAsMap(Sets.newHashSet(entitySetCollection.template.values))

        val roles = appRoles.associate {

            /* Create the role if it doesn't already exist */
            val rolePrincipal = Principal(PrincipalType.ROLE,
                    getNextAvailableName("$organizationId|${it.title}"))
            val role = Role(Optional.empty(), organizationId, rolePrincipal, it.title, Optional.of(it.description!!))

            val aclKey = if (principalsService.createSecurablePrincipalIfNotExists(userPrincipal, role))
                role.aclKey
            else
                principalsService.lookup(rolePrincipal)

            /* Track permissions that need to be granted to the role */
            permissionsToGrant[AceKey(AclKey(entitySetCollectionId), rolePrincipal)] = EnumSet.of(Permission.READ)
            permissionsToGrant[AceKey(AclKey(entitySetCollectionId), appPrincipal)] = EnumSet.of(Permission.READ)
            it.permissions.forEach { (permission, requiredTemplateTypes) ->

                requiredTemplateTypes.forEach { (templateTypeId, maybePropertyTypeIds) ->
                    val entitySetId = entitySetCollection.template.getValue(templateTypeId)

                    val propertyTypeIds = maybePropertyTypeIds
                            .orElse(entityTypesById.getValue(entitySetsById.getValue(entitySetId).entityTypeId).properties)

                    propertyTypeIds.map { id -> AclKey(entitySetId, id) }.plusElement(AclKey(entitySetId)).forEach { ak ->
                        val aceKey = AceKey(ak, rolePrincipal)
                        val permissionEnumSet = permissionsToGrant.getOrDefault(aceKey, EnumSet.noneOf(Permission::class.java))
                        permissionEnumSet.add(permission)
                        permissionsToGrant[aceKey] = permissionEnumSet
                        permissionsToGrant[AceKey(ak, appPrincipal)] = EnumSet.of(Permission.READ)
                    }
                }
            }

            it.id!! to aclKey!!
        }.toMutableMap()

        /* Grant the required permissions to app roles */
        authorizationService.setPermissions(permissionsToGrant)

        return roles
    }

    private fun getNextAvailableName(name: String): String {

        var nameAttempt = name
        var counter = 1

        while (reservations.isReserved(nameAttempt)) {
            nameAttempt = name + "_" + counter
            counter++
        }

        return nameAttempt
    }

    fun installAppAndCreateEntitySetCollection(
            appId: UUID,
            organizationId: UUID,
            appInstallation: AppInstallation,
            principal: Principal) {
        val app = getApp(appId)

        Preconditions.checkNotNull(app, "The requested app with id $appId does not exist.")

        var entitySetCollectionId = appInstallation.entitySetCollectionId

        if (entitySetCollectionId == null) {
            entitySetCollectionId = collectionsManager.createEntitySetCollection(EntitySetCollection(
                    Optional.empty<UUID>(),
                    getNextAvailableName(app.name + "_" + organizationId),
                    appInstallation.prefix + " " + app.title,
                    Optional.of<String>(app.description),
                    app.entityTypeCollectionId,
                    appInstallation.template!!,
                    ImmutableSet.of<String>(),
                    organizationId
            ), true)
        }

        var settings = appInstallation.settings
        if (settings == null) {
            settings = app.defaultSettings
        }

        installApp(app, organizationId, entitySetCollectionId, principal, settings!!)
    }

    fun installApp(
            app: App,
            organizationId: UUID,
            entitySetCollectionId: UUID,
            principal: Principal,
            settings: MutableMap<String, Any>) {

        val appId = app.id

        val appConfigKey = AppConfigKey(appId, organizationId)
        Preconditions.checkArgument(!appConfigs.containsKey(appConfigKey),
                "App {} is already installed for organization {}",
                appId,
                organizationId)

        val nonexistentKeys = Sets.difference(settings.keys, app.defaultSettings.keys)
        Preconditions.checkArgument(nonexistentKeys.isEmpty(),
                "Cannot create app {} in organization {} with settings containing keys that do not exist: {}",
                appId,
                organizationId,
                nonexistentKeys)

        val appPrincipal = getAppPrincipal(appConfigKey)

        principalsService.createSecurablePrincipalIfNotExists(principal, SecurablePrincipal(
                Optional.empty(),
                appPrincipal,
                app.title,
                Optional.of(app.description)))

        val appRoles = createRolesForApp(app.appRoles,
                organizationId,
                entitySetCollectionId,
                appPrincipal,
                principal)

        appConfigs[appConfigKey] = AppTypeSetting(principalsService.lookup(appPrincipal)[0],
                entitySetCollectionId,
                appRoles,
                settings)

        organizationService.addAppToOrg(organizationId, appId)
    }

    private fun getAppPrincipal(appConfigKey: AppConfigKey): Principal {
        return Principal(PrincipalType.APP, getAppPrincipalId(appConfigKey.appId, appConfigKey.organizationId))
    }

    fun uninstallApp(appId: UUID, organizationId: UUID) {
        val appConfigKey = AppConfigKey(appId, organizationId)

        organizationService.removeAppFromOrg(organizationId, appId)
        deleteAppPrincipal(appConfigKey)
        appConfigs.delete(appConfigKey)
    }

    private fun deleteAppPrincipal(appConfigKey: AppConfigKey) {
        principalsService.deletePrincipal(principalsService.lookup(getAppPrincipal(appConfigKey)))
    }


    @Deprecated("This should be phased out once apps have been updated to use getAvailableConfigs")
    fun getAvailableConfigsOld(appId: UUID, principals: Set<Principal>): List<HistoricalAppConfig> {
        val userAppConfigs = getAvailableConfigs(appId, principals)

        val app = getApp(appId)
        val entitySetCollectionsById = collectionsManager.getEntitySetCollections(userAppConfigs.map { it.entitySetCollectionId }.toSet())
        val entityTypeCollectionsById = collectionsManager.getEntityTypeCollections(entitySetCollectionsById.values.map { it.entityTypeCollectionId }.toSet())
        val organizationsById = organizationService.getOrganizations(userAppConfigs.stream().map { it.organizationId }).associateBy { it.id }


        val entitySetCollectionTemplates: Map<UUID, Map<String, UUID>> = entitySetCollectionsById.values.associate {
            it.id to entityTypeCollectionsById.getValue(it.entityTypeCollectionId).template.associate { type -> type.name to it.template.getValue(type.id) }
        }

        return userAppConfigs.map {
            HistoricalAppConfig(
                    Optional.of(appId),
                    getAppPrincipal(AppConfigKey(appId, it.organizationId)),
                    app.title,
                    Optional.of(app.description),
                    appId,
                    organizationsById.getValue(it.organizationId),
                    // note: the permissions field here is not actually used by frontend apps, so we don't need to worry about loading real values here
                    // since this method only exists as a stopgap for the apps transition.
                    entitySetCollectionTemplates.getValue(it.entitySetCollectionId).mapValues { setting -> HistoricalAppTypeSetting(setting.value, EnumSet.noneOf(Permission::class.java)) }
            )
        }
    }

    fun getAvailableConfigs(appId: UUID, principals: Set<Principal>): List<UserAppConfig> {

        val principalAclKeys = principalsService.lookup(principals)

        val aclKeysByPrincipalType = principalAclKeys.entries.groupBy { it.key.type }.mapValues { it.value.map { entry -> entry.value } }

        return appConfigs.entrySet(Predicates.and(
                Predicates.equal<AppConfigKey, AppTypeSetting>(AppConfigMapstore.APP_ID, appId),
                Predicates.`in`<AppConfigKey, AppTypeSetting>(AppConfigMapstore.ORGANIZATION_ID, *(aclKeysByPrincipalType[PrincipalType.ORGANIZATION]
                        ?: listOf()).map { it[0] }.toTypedArray()))).map {

            val organizationId = it.key.organizationId
            val setting = it.value

            val entitySetCollectionId = setting.entitySetCollectionId

            val availableRoles = setting.roles.entries.filter { roleEntry ->
                (aclKeysByPrincipalType[PrincipalType.ROLE] ?: listOf<UUID>()).contains(roleEntry.value)
            }.map { roleEntry -> roleEntry.key }.toSet()

            UserAppConfig(organizationId,
                    entitySetCollectionId,
                    availableRoles,
                    setting.settings)

        }.filter { it.roles.isNotEmpty() }.toList()
    }

    fun updateAppRolePermissions(
            appId: UUID,
            roleId: UUID,
            permissions: Map<Permission, Map<UUID, Optional<Set<UUID>>>>) {

        val app = getApp(appId)

        Preconditions.checkState(app.appRoles.any { it.id == roleId },
                "App {} does not contain a role with id {}.",
                appId,
                roleId)
        val templateTypeIds = permissions.values.flatMap { it.keys }.toSet()
        val nonexistentTemplateTypeIds = Sets.difference(templateTypeIds,
                entityTypeCollections.getValue(app.entityTypeCollectionId).template.map { it.id }.toSet())

        Preconditions.checkState(nonexistentTemplateTypeIds.isEmpty(),
                "Could not update role {} permissions for app {} because the following templateTypeIds are not present in the EntityTypeCollection: ",
                roleId,
                appId,
                nonexistentTemplateTypeIds)

        apps.executeOnKey(appId, UpdateAppRolePermissionsProcessor(roleId, permissions))
    }

    fun updateAppMetadata(appId: UUID, metadataUpdate: MetadataUpdate) {
        if (metadataUpdate.name.isPresent) {
            reservations.renameReservation(appId, metadataUpdate.name.get())
        }

        apps.executeOnKey(appId, UpdateAppMetadataProcessor(metadataUpdate))
        eventBus.post(AppCreatedEvent(getApp(appId)))
    }

    fun updateDefaultAppSettings(appId: UUID, defaultSettings: MutableMap<String, Any>) {

        val app = getApp(appId)
        val oldSettings = app.defaultSettings

        val newKeys = Sets.difference(oldSettings.keys, defaultSettings.keys)
        val deletedKeys = Sets.difference(defaultSettings.keys, oldSettings.keys)

        val settingsToAdd = defaultSettings.filter { newKeys.contains(it.key) }

        val appConfigKeysToUpdate = appConfigs.keySet(Predicates.equal(AppConfigMapstore.APP_ID, appId))

        appConfigs.executeOnKeys(appConfigKeysToUpdate, UpdateAppConfigSettingsProcessor(settingsToAdd, deletedKeys))
        apps.executeOnKey(appId, UpdateDefaultAppSettingsProcessor(defaultSettings))
    }

    fun updateAppConfigSettings(appId: UUID, organizationId: UUID, newAppSettings: Map<String, Any>) {

        val app = getApp(appId)

        val appConfigKey = AppConfigKey(appId, organizationId)
        Preconditions.checkArgument(appConfigs.containsKey(appConfigKey),
                "App {} is not installed for organization {}.",
                appId,
                organizationId)

        val nonexistentKeys = Sets.difference(newAppSettings.keys, app.defaultSettings.keys)
        Preconditions.checkArgument(nonexistentKeys.isEmpty(),
                "Cannot update app {} in organization {} with settings containing keys that do not exist: {}",
                appId,
                organizationId,
                nonexistentKeys)

        appConfigs.executeOnKey(appConfigKey,
                UpdateAppConfigSettingsProcessor(newAppSettings, ImmutableSet.of()))

    }

    private fun validateAppAndRoles(appId: UUID, appRole: AppRole) {
        val app = getApp(appId)
        app.addRole(appRole)
        validateAppAndRoles(app)
    }

    private fun validateAppAndRoles(app: App) {
        val entityTypeCollection = collectionsManager
                .getEntityTypeCollection(app.entityTypeCollectionId)

        val templateTypeIds = entityTypeCollection.template.map { it.id }.toSet()

        app.appRoles.forEach { (_, name, _, _, permissions) ->
            permissions.values.flatMap { it.keys }.forEach { templateTypeId ->
                if (!templateTypeIds.contains(templateTypeId)) {
                    throw IllegalArgumentException("Role $name cannot be created for app ${app.name} because permissions " +
                            "were requested on an invalid collection template type: $templateTypeId")
                }
            }
        }
    }

}
