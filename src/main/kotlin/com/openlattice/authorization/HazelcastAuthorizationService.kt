package com.openlattice.authorization

import com.codahale.metrics.annotation.Timed
import com.google.common.collect.HashMultimap
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Maps
import com.google.common.collect.SetMultimap
import com.google.common.eventbus.EventBus
import com.hazelcast.aggregation.Aggregators
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.assembler.events.MaterializePermissionChangeEvent
import com.openlattice.authorization.aggregators.AuthorizationSetAggregator
import com.openlattice.authorization.aggregators.PrincipalAggregator
import com.openlattice.authorization.mapstores.PermissionMapstore
import com.openlattice.authorization.mapstores.PermissionMapstore.ACL_KEY_INDEX
import com.openlattice.authorization.processors.AuthorizationEntryProcessor
import com.openlattice.authorization.processors.PermissionMerger
import com.openlattice.authorization.processors.PermissionRemover
import com.openlattice.authorization.processors.SecurableObjectTypeUpdater
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.authorization.util.toAceKeys
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.PrincipalSet
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.OffsetDateTime
import java.util.*
import java.util.function.Function
import java.util.stream.Collectors
import java.util.stream.Stream

@Service
class HazelcastAuthorizationService(
        hazelcastInstance: HazelcastInstance,
        val eventBus: EventBus
) : AuthorizationManager {

    private val securableObjectTypes: IMap<AclKey, SecurableObjectType> = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(hazelcastInstance)
    private val aces: IMap<AceKey, AceValue> = HazelcastMap.PERMISSIONS.getMap(hazelcastInstance)

    companion object {
        private val logger = LoggerFactory.getLogger(HazelcastAuthorizationService::class.java)

        private fun noAccess(permissions: EnumSet<Permission>): EnumMap<Permission, Boolean> {
            val pm = EnumMap<Permission, Boolean>(Permission::class.java)
            permissions.forEach { pm[it] = false }
            return pm
        }

        private fun matches(aclKeys: Collection<AclKey>, principals: Set<Principal>): Predicate<AceKey, AceValue> {
            return Predicates.and(hasAnyAclKeys(aclKeys), hasAnyPrincipals(principals)) as Predicate<AceKey, AceValue>
        }

        private fun matches(aclKey: AclKey, permissions: EnumSet<Permission>): Predicate<AceKey, AceValue> {
            return Predicates.and(hasAclKey(aclKey), hasExactPermissions(permissions)) as Predicate<AceKey, AceValue>
        }

        private fun hasExactPermissions(permissions: EnumSet<Permission>): Predicate<AceKey, AceValue> {

            val subPredicates = permissions
                    .map { Predicates.equal(PermissionMapstore.PERMISSIONS_INDEX, it) }
                    .toTypedArray()

            return Predicates.and(*subPredicates) as Predicate<AceKey, AceValue>
        }

        private fun hasAnyPrincipals(principals: Collection<Principal>): Predicate<AceKey, AceValue> {
            return Predicates.`in`(PermissionMapstore.PRINCIPAL_INDEX, *principals.toTypedArray()) as Predicate<AceKey, AceValue>
        }

        private fun hasAnyAclKeys(aclKeys: Collection<AclKey>): Predicate<AceKey, AceValue> {
            return Predicates.`in`(ACL_KEY_INDEX, *aclKeys.map { it.index }.toTypedArray()) as Predicate<AceKey, AceValue>
        }

        private fun hasAclKey(aclKey: AclKey): Predicate<AceKey, AceValue> {
            return Predicates.equal(ACL_KEY_INDEX, aclKey.index) as Predicate<AceKey, AceValue>
        }

        private fun hasType(objectType: SecurableObjectType): Predicate<AceKey, AceValue> {
            return Predicates.equal(PermissionMapstore.SECURABLE_OBJECT_TYPE_INDEX, objectType) as Predicate<AceKey, AceValue>
        }

        private fun hasPrincipal(principal: Principal): Predicate<AceKey, AceValue> {
            return Predicates.equal(PermissionMapstore.PRINCIPAL_INDEX, principal) as Predicate<AceKey, AceValue>
        }

        private fun hasPrincipalType(type: PrincipalType): Predicate<AceKey, AceValue> {
            return Predicates.equal(PermissionMapstore.PRINCIPAL_TYPE_INDEX, type) as Predicate<AceKey, AceValue>
        }
    }

    /** Set Securable Object Type **/


    override fun setSecurableObjectTypes(aclKeys: Set<AclKey>, objectType: SecurableObjectType) {
        securableObjectTypes.putAll(aclKeys.associateWith { objectType })
        aces.executeOnEntries(SecurableObjectTypeUpdater(objectType), hasAnyAclKeys(aclKeys))
    }

    override fun setSecurableObjectType(aclKey: AclKey, objectType: SecurableObjectType) {
        securableObjectTypes[aclKey] = objectType
        aces.executeOnEntries(SecurableObjectTypeUpdater(objectType), hasAclKey(aclKey))
    }

    /** Add Permissions **/

    override fun addPermission(key: AclKey, principal: Principal, permissions: EnumSet<Permission>) {
        addPermission(key, principal, permissions, OffsetDateTime.MAX)
    }

    override fun addPermission(key: AclKey, principal: Principal, permissions: EnumSet<Permission>, expirationDate: OffsetDateTime) {
        //TODO: We should do something better than reading the securable object type.
        val securableObjectType = getDefaultObjectType(securableObjectTypes, key)

        aces.executeOnKey(AceKey(key, principal), PermissionMerger(permissions, securableObjectType, expirationDate))

        signalMaterializationPermissionChange(key, principal, permissions, securableObjectType)
    }

    override fun addPermissions(
            keys: Set<AclKey>,
            principal: Principal,
            permissions: EnumSet<Permission>,
            securableObjectType: SecurableObjectType
    ) {
        addPermissions(keys, principal, permissions, securableObjectType, OffsetDateTime.MAX)
    }

    override fun addPermissions(
            keys: Set<AclKey>,
            principal: Principal,
            permissions: EnumSet<Permission>,
            securableObjectType: SecurableObjectType,
            expirationDate: OffsetDateTime
    ) {
        val aceKeys = toAceKeys(keys, principal)
        aces.executeOnKeys(aceKeys, PermissionMerger(permissions, securableObjectType, expirationDate))
    }

    override fun addPermissions(acls: List<Acl>) {
        val updates = getAceValueToAceKeyMap(acls)
        updates.keySet().forEach {
            val aceKeys = updates[it]
            aces.executeOnKeys(aceKeys, PermissionMerger(it.permissions, it.securableObjectType, it.expirationDate))
        }
    }

    /** Remove Permissions **/

    override fun removePermissions(acls: List<Acl>) {
        acls
                .map {
                    AclKey(it.aclKey) to it.aces
                            .filter { ace -> ace.permissions.contains(Permission.OWNER) }
                            .map { ace -> ace.principal }
                            .toSet()
                }
                .filter { (_, owners) -> owners.isNotEmpty() }
                .forEach { (aclKey, owners) -> ensureAclKeysHaveOtherUserOwners(ImmutableSet.of(aclKey), owners) }


        val updates = getAceValueToAceKeyMap(acls)

        updates.keySet().forEach {
            aces.executeOnKeys(updates[it], PermissionRemover(it.permissions))
        }
    }


    override fun removePermission(
            key: AclKey,
            principal: Principal,
            permissions: EnumSet<Permission>
    ) {
        if (permissions.contains(Permission.OWNER)) {
            ensureAclKeysHaveOtherUserOwners(setOf(key), setOf(principal))
        }

        signalMaterializationPermissionChange(
                key, principal, permissions, getDefaultObjectType(securableObjectTypes, key)
        )

        aces.executeOnKey(AceKey(key, principal), PermissionRemover(permissions))
    }

    override fun deletePermissions(aclKey: AclKey) {
        securableObjectTypes.delete(aclKey)
        aces.removeAll(hasAclKey(aclKey))
    }

    override fun deletePrincipalPermissions(principal: Principal) {
        aces.removeAll(hasPrincipal(principal))
    }

    /** Set Permissions **/

    override fun setPermissions(acls: List<Acl>) {
        val types = getSecurableObjectTypeMapForAcls(acls)

        val updates = mutableMapOf<AceKey, AceValue>()

        acls.forEach {
            val aclKey = AclKey(it.aclKey)
            val securableObjectType = getDefaultObjectType(types, aclKey)

            it.aces.forEach { ace: Ace ->
                val principal = ace.principal
                val permissions = ace.permissions
                updates[AceKey(aclKey, principal)] = AceValue(permissions, securableObjectType, ace.expirationDate)
                signalMaterializationPermissionChange(aclKey, principal, permissions, securableObjectType)
            }
        }

        aces.putAll(updates)
    }

    override fun setPermission(
            key: AclKey,
            principal: Principal,
            permissions: EnumSet<Permission>
    ) {
        setPermission(key, principal, permissions, OffsetDateTime.MAX)
    }

    override fun setPermission(
            key: AclKey,
            principal: Principal,
            permissions: EnumSet<Permission>,
            expirationDate: OffsetDateTime
    ) {
        if (!permissions.contains(Permission.OWNER)) {
            ensureAclKeysHaveOtherUserOwners(setOf(key), setOf(principal))
        }

        //This should be a rare call to overwrite all permissions, so it's okay to do a read before write.
        val securableObjectType = getDefaultObjectType(securableObjectTypes, key)
        signalMaterializationPermissionChange(key, principal, permissions, securableObjectType)
        aces[AceKey(key, principal)] = AceValue(permissions, securableObjectType, expirationDate)
    }

    override fun setPermission(aclKeys: Set<AclKey>, principals: Set<Principal>, permissions: EnumSet<Permission>) {
        //This should be a rare call to overwrite all permissions, so it's okay to do a read before write.
        if (!permissions.contains(Permission.OWNER)) {
            ensureAclKeysHaveOtherUserOwners(aclKeys, principals)
        }

        val securableObjectTypesForAclKeys = securableObjectTypes.getAll(aclKeys)
        val newPermissions: MutableMap<AceKey, AceValue> = HashMap(aclKeys.size * principals.size)

        aclKeys.forEach {
            val objectType = getDefaultObjectType(securableObjectTypesForAclKeys, it)
            val aceValue = AceValue(permissions, objectType, OffsetDateTime.MAX)

            principals.forEach { principal ->
                newPermissions[AceKey(it, principal)] = aceValue
                signalMaterializationPermissionChange(it, principal, permissions, objectType)
            }
        }

        aces.putAll(newPermissions)
    }

    override fun setPermissions(permissions: Map<AceKey, EnumSet<Permission>>) {

        permissions.entries
                .filter { entry -> !entry.value.contains(Permission.OWNER) }
                .groupBy { e -> e.key.aclKey }
                .mapValues { it.value.map { entry -> entry.key.principal }.toSet() }
                .forEach { (aclKey, principals) -> ensureAclKeysHaveOtherUserOwners(setOf(aclKey), principals) }

        val securableObjectTypesForAclKeys = securableObjectTypes.getAll(permissions.keys.map { it.aclKey }.toSet())

        val newPermissions: MutableMap<AceKey, AceValue> = Maps.newHashMap()

        permissions.forEach { (aceKey: AceKey, acePermissions: EnumSet<Permission>) ->
            val aclKey = aceKey.aclKey
            val objectType = getDefaultObjectType(securableObjectTypesForAclKeys, aclKey)
            newPermissions[aceKey] = AceValue(acePermissions, objectType, OffsetDateTime.MAX)
            signalMaterializationPermissionChange(aclKey, aceKey.principal, acePermissions, objectType)
        }

        aces.putAll(newPermissions)
    }

    /*** AUTH CHECKS ***/

    @Timed
    override fun authorize(
            requests: Map<AclKey, EnumSet<Permission>>,
            principals: Set<Principal>
    ): MutableMap<AclKey, EnumMap<Permission, Boolean>> {

        val permissionMap = requests.mapValues { noAccess(it.value) }.toMutableMap()

        val aceKeys = requests.keys.flatMap { aclKey ->
            principals.map { principal -> AceKey(aclKey, principal) }
        }.toSet()

        aces.executeOnKeys(aceKeys, AuthorizationEntryProcessor()).forEach { (aceKey: AceKey, permissions: Any) ->

            val aclKeyPermissions = permissionMap.getValue(aceKey.aclKey)

            (permissions as DelegatedPermissionEnumSet).filter { aclKeyPermissions.contains(it) }.forEach {
                aclKeyPermissions[it] = true
            }

            permissionMap[aceKey.aclKey] = aclKeyPermissions
        }
        return permissionMap
    }

    @Timed
    override fun accessChecksForPrincipals(accessChecks: Set<AccessCheck>, principals: Set<Principal>): Stream<Authorization> {

        val requests: MutableMap<AclKey, EnumSet<Permission>> = Maps.newLinkedHashMapWithExpectedSize(accessChecks.size)

        accessChecks.forEach {
            val p = requests.getOrDefault(it.aclKey, EnumSet.noneOf(Permission::class.java))
            p.addAll(it.permissions)
            requests[it.aclKey] = p
        }

        return authorize(requests, principals)
                .entries
                .stream()
                .map { e -> Authorization(e.key, e.value) }
    }

    @Timed
    override fun checkIfHasPermissions(
            key: AclKey,
            principals: Set<Principal>,
            requiredPermissions: EnumSet<Permission>
    ): Boolean {

        val aceKeys = principals.map { AceKey(key, it) }.toSet()

        return aces.executeOnKeys(aceKeys, AuthorizationEntryProcessor())
                .values
                .flatMap { it as DelegatedPermissionEnumSet }
                .toSet()
                .containsAll(requiredPermissions)
    }

    @Timed
    override fun getSecurableObjectSetsPermissions(
            aclKeySets: Collection<Set<AclKey>>,
            principals: Set<Principal>
    ): Map<Set<AclKey>, EnumSet<Permission>> {
        return aclKeySets.parallelStream().collect(Collectors.toMap<Set<AclKey>, Set<AclKey>, EnumSet<Permission>>(
                Function.identity(),
                Function { getSecurableObjectSetPermissions(it, principals) }
        ))
    }

    @Timed
    override fun getSecurableObjectPermissions(
            key: AclKey,
            principals: Set<Principal>
    ): Set<Permission> {
        val objectPermissions = EnumSet.noneOf(Permission::class.java)
        val aceKeys = principals.map { AceKey(key, it) }.toSet()
        aces.getAll(aceKeys).values.mapNotNull { it.permissions }.forEach { objectPermissions.addAll(it) }

        return objectPermissions
    }

    @Timed
    override fun getAuthorizedObjectsOfType(
            principal: Principal,
            objectType: SecurableObjectType,
            permissions: EnumSet<Permission>
    ): Stream<AclKey> {
        return getAuthorizedObjectsOfType(setOf(principal), objectType, permissions)
    }

    @Timed
    override fun getAuthorizedObjectsOfType(
            principals: Set<Principal>,
            objectType: SecurableObjectType,
            permissions: EnumSet<Permission>): Stream<AclKey> {
        val principalPredicate = if (principals.size == 1) hasPrincipal(principals.first()) else hasAnyPrincipals(principals)
        val p = Predicates.and(
                principalPredicate,
                hasType(objectType),
                hasExactPermissions(permissions)
        ) as Predicate<AceKey, AceValue>

        return aces.keySet(p)
                .stream()
                .map { it.aclKey }
                .distinct()
    }

    @Timed
    override fun getAuthorizedObjectsOfType(
            principals: Set<Principal>,
            objectType: SecurableObjectType,
            permissions: EnumSet<Permission>,
            additionalFilter: Predicate<*, *>): Stream<AclKey> {
        val p = Predicates.and(
                hasAnyPrincipals(principals),
                hasType(objectType),
                hasExactPermissions(permissions),
                additionalFilter) as Predicate<AceKey, AceValue>
        return aces.keySet(p)
                .stream()
                .map { obj: AceKey -> obj.aclKey }
                .distinct()
    }

    @Timed
    override fun getAllSecurableObjectPermissions(key: AclKey): Acl {
        val acesWithPermissions = aces.entrySet(hasAclKey(key))
                .map { Ace(it.key.principal, it.value.permissions) }
                .toSet()

        return Acl(key, acesWithPermissions)
    }

    override fun getAuthorizedPrincipalsOnSecurableObject(key: AclKey, permissions: EnumSet<Permission>): Set<Principal> {
        val principalMap = mutableMapOf(key to PrincipalSet(mutableSetOf()))

        return aces.aggregate(PrincipalAggregator(principalMap), matches(key, permissions))
                .getResult()
                .getValue(key)
                .unwrap()
    }

    @Timed
    override fun getSecurableObjectOwners(key: AclKey): Set<Principal> {
        return getAuthorizedPrincipalsOnSecurableObject(key, EnumSet.of(Permission.OWNER))
    }

    @Timed
    override fun getOwnersForSecurableObjects(aclKeys: Collection<AclKey>): SetMultimap<AclKey, Principal> {
        val result: SetMultimap<AclKey, Principal> = HashMultimap.create()

        aces.keySet(Predicates.and(hasAnyAclKeys(aclKeys), hasExactPermissions(EnumSet.of(Permission.OWNER))))
                .forEach { result.put(it.aclKey, it.principal) }

        return result
    }


    /** Private Helpers **/

    private fun ensureAclKeysHaveOtherUserOwners(aclKeys: Set<AclKey>, principals: Set<Principal>) {
        val userPrincipals = principals.stream().filter { p: Principal -> p.type == PrincipalType.USER }
                .collect(Collectors.toSet())
        if (userPrincipals.size > 0) {

            val allOtherUserOwnersPredicate = Predicates.and(
                    hasAnyAclKeys(aclKeys),
                    hasExactPermissions(EnumSet.of(Permission.OWNER)),
                    Predicates.not(hasAnyPrincipals(userPrincipals)),
                    hasPrincipalType(PrincipalType.USER)
            ) as Predicate<AceKey, AceValue>

            val allOtherUserOwnersCount: Long = aces.aggregate(Aggregators.count<Map.Entry<AceKey, AceValue>>(), allOtherUserOwnersPredicate)
            check(allOtherUserOwnersCount != 0L) {
                "Unable to remove owner permissions as a securable object will be left without an owner of " +
                        "type USER"
            }
        }
    }

    private fun getAceValueToAceKeyMap(acls: List<Acl>): SetMultimap<AceValue, AceKey> {
        val types = getSecurableObjectTypeMapForAcls(acls)

        val map: SetMultimap<AceValue, AceKey> = HashMultimap.create()
        acls.forEach { acl: Acl ->

            val aclKey = AclKey(acl.aclKey)
            val securableObjectType = getDefaultObjectType(types, aclKey)

            acl.aces.forEach {
                map.put(AceValue(it.permissions, securableObjectType, it.expirationDate), AceKey(aclKey, it.principal))
            }
        }
        return map
    }

    private fun getSecurableObjectSetPermissions(
            aclKeySet: Set<AclKey>,
            principals: Set<Principal>
    ): EnumSet<Permission> {

        val authorizationsMap = aclKeySet
                .associateWith { EnumSet.noneOf(Permission::class.java) }
                .toMutableMap()

        return aces.aggregate(AuthorizationSetAggregator(authorizationsMap), matches(aclKeySet, principals))
    }

    private fun signalMaterializationPermissionChange(
            key: AclKey,
            principal: Principal,
            permissions: EnumSet<Permission>,
            securableObjectType: SecurableObjectType
    ) {
        // if there is a change in materialization permission for a property type or an entity set for an organization
        // principal, we need to flag it
        if (permissions.contains(Permission.MATERIALIZE)
                && principal.type == PrincipalType.ORGANIZATION
                && (securableObjectType == SecurableObjectType.PropertyTypeInEntitySet || securableObjectType == SecurableObjectType.EntitySet)) {
            eventBus.post(MaterializePermissionChangeEvent(principal, setOf(key[0]), securableObjectType))
        }
    }

    private fun getSecurableObjectTypeMapForAcls(acls: Collection<Acl>): Map<AclKey, SecurableObjectType> {
        return securableObjectTypes.getAll(acls.map { AclKey(it.aclKey) }.toSet())
    }

    private fun getDefaultObjectType(types: Map<AclKey, SecurableObjectType>, aclKey: AclKey): SecurableObjectType {
        val securableObjectType = types.getOrDefault(aclKey, SecurableObjectType.Unknown)
        if (securableObjectType == SecurableObjectType.Unknown) {
            logger.warn("Unrecognized object type for acl key {} key ", aclKey)
        }

        return securableObjectType
    }

}