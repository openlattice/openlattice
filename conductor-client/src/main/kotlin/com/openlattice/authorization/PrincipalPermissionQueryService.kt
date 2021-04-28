package com.openlattice.authorization

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.authorization.mapstores.PrincipalTreesMapstore
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresColumn.ACL_KEY
import com.openlattice.postgres.PostgresColumn.PRINCIPAL_ID
import com.openlattice.postgres.PostgresTable.PERMISSIONS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import java.util.*


class PrincipalPermissionQueryService(
        val hazelcast: HazelcastInstance,
        val hds: HikariDataSource,
        val principalsMapManager: PrincipalsMapManager
) {

    private val principalTrees = HazelcastMap.PRINCIPAL_TREES.getMap(hazelcast)
    private val principals = HazelcastMap.PRINCIPALS.getMap(hazelcast)
    private val permissions = HazelcastMap.PERMISSIONS.getMap(hazelcast)

    companion object {
        private val CREATE_AGG_SQL = """
            CREATE OR REPLACE AGGREGATE array_cat_agg(anyarray) (
              SFUNC=array_cat,
              STYPE=anyarray
            )
    """.trimIndent()
    }

    init {
        initializePostgresFunction()
    }

    private fun hasAnySecurablePrincipal(aclKeys: Collection<AclKey>): Predicate<AclKey, AclKeySet> {
        return Predicates.`in`(PrincipalTreesMapstore.INDEX, *aclKeys.map { it.index }.toTypedArray())
    }

    private fun hasPrincipalType(principalType: PrincipalType): Predicate<AclKey, SecurablePrincipal> {
        return PrincipalsMapManager.hasPrincipalType(principalType)
    }

    private fun hasSecurablePrincipal(principalAclKey: AclKey): Predicate<AclKey, AclKeySet> {
        return Predicates.equal(PrincipalTreesMapstore.INDEX, principalAclKey.index)
    }

    private fun getAclKey(principal: Principal): AclKey {
        return principalsMapManager.getAclKeyByPrincipal(setOf(principal)).getValue(principal)
    }

    fun getUserPrincipalsWithPrincipals(p: Set<Principal>): Set<SecurablePrincipal> {
        val parentPrincipalAclKeys = principalTrees.keySet(hasAnySecurablePrincipal(
                principalsMapManager.getAclKeyByPrincipal(p).values
        ))
        return principals.getAll(parentPrincipalAclKeys).values.filter { it.principalType == PrincipalType.USER }.toSet()
    }


    private fun getUserPrincipalTreesExcludingPrincipals(
            sps: Set<SecurablePrincipal>,
            aclKeysToExclude: Set<AclKey>
    ): Map<String, Set<String>> {
        val aclKeyPrincipals = mutableMapOf<AclKey, Set<AclKey>>()

        // Bulk load all relevant principal trees from hazelcast
        var nextLayer = sps.mapTo(mutableSetOf()) { it.aclKey }
        while (nextLayer.isNotEmpty()) {
            //Don't load what's already been loaded.
            val nextLayerMap = principalTrees.getAll(nextLayer - aclKeyPrincipals.keys).mapValues {
                it.value.filter { ak -> !aclKeysToExclude.contains(ak) }.toSet()
            }
            nextLayer = nextLayerMap.values.flatMapTo(mutableSetOf()) { it }
            aclKeyPrincipals.putAll(nextLayerMap)
        }

        // Map all loaded principals to SecurablePrincipals
        val aclKeysToPrincipals = principals.getAll(aclKeyPrincipals.keys + aclKeyPrincipals.values.flatten())

        // Map each SecurablePrincipal to all its aclKey children from the in-memory map, and from there a SortedPrincipalSet
        return sps.associate { sp ->
            val childAclKeys = mutableSetOf<AclKey>(sp.aclKey) //Need to include self.
            aclKeyPrincipals.getOrDefault(sp.aclKey, AclKeySet()).forEach { childAclKeys.add(it) }

            var nextAclKeyLayer: Set<AclKey> = childAclKeys

            while (nextAclKeyLayer.isNotEmpty()) {
                nextAclKeyLayer = (nextAclKeyLayer.flatMapTo(mutableSetOf<AclKey>()) {
                    aclKeyPrincipals[it] ?: setOf()
                }) - childAclKeys
                childAclKeys += nextAclKeyLayer
            }

            val principalIds = childAclKeys.mapNotNull { aclKey -> aclKeysToPrincipals[aclKey]?.principal?.id }.toSet()

            sp.principal.id to principalIds
        }
    }

    private fun findAllUsersWithPrincipals(aclKeys: Set<AclKey>): Set<SecurablePrincipal> {
        val parentAclKeys = aclKeys.toMutableSet()

        var layer = principalTrees.keySet(hasAnySecurablePrincipal(parentAclKeys)) - parentAclKeys
        while (layer.isNotEmpty()) {
            parentAclKeys.addAll(layer)
            layer = principalTrees.keySet(hasAnySecurablePrincipal(parentAclKeys)) - parentAclKeys
        }

        return principals.getAll(parentAclKeys).values.filter { it.principalType == PrincipalType.USER }.toSet()
    }

    fun resolvePermissionChangeOnPrincipalTreeChange(
            principalAclKeyBeingMoved: AclKey,
            parentAclKeys: Set<AclKey>
    ): Map<AceKey, EnumSet<Permission>> {

        // get principal tree of [principalAclKeyBeingMoved]
        val removedPrincipalTree = principals.getAll(
                principalTrees.getOrDefault(principalAclKeyBeingMoved, AclKeySet())
        ).mapTo(mutableSetOf()) { it.value.principal.id }
        val removedPrincipalId = principals.getValue(principalAclKeyBeingMoved).principal.id

        // get principals that have [principalAclKeyBeingMoved]
        val usersAffected = findAllUsersWithPrincipals(parentAclKeys)
        if (usersAffected.isEmpty()) {
            return mapOf()
        }

        // get new principal trees for affected users, excluding principalAclKeyBeingMoved
        val affectedPrincipalIdTrees = getUserPrincipalTreesExcludingPrincipals(
                usersAffected,
                setOf(principalAclKeyBeingMoved)
        )

        val sql = permissionDiffSql(affectedPrincipalIdTrees, removedPrincipalId to removedPrincipalTree)

        return BasePostgresIterable(StatementHolderSupplier(hds, sql)) {
            val principalId = ResultSetAdapters.principalId(it)
            val aclKey = ResultSetAdapters.aclKey(it)
            val permissions = ResultSetAdapters.permissions(it)

            AceKey(aclKey, Principal(PrincipalType.USER, principalId)) to permissions
        }.toMap()
    }

    private val PRINCIPAL_TREES_CTE = "principal_trees_cte"
    private val PRINCIPAL_IDS = "principal_ids"
    private val REMOVED_PRINCIPAL_TREE_PERMS = "removed_principal_tree_permissions"
    private val PRINCIPAL_PERMISSIONS = "principal_permissions"
    private val PERMISSIONS_IN_QUESTION = "permissions_in_question"

    private fun quotePrincipalId(principalId: String): String {
        return "'${principalId.replace("'", "''")}'"
    }

    private fun principalIdsCTE(principalIds: Map<String, Set<String>>): String {
        val escapedPrincipalIds = principalIds.keys.map { it.replace("'", "''") }
        return """
            $PRINCIPAL_IDS (principal_id) AS (VALUES ${escapedPrincipalIds.joinToString { "(${quotePrincipalId(it)})" }})
        """.trimIndent()
    }

    private fun principalPermissionsCTE(principalIds: Map<String, Set<String>>, aclKeys: Set<AclKey>?): String {
        val pairs = principalIds.flatMap { (principalId, childPrincipalIds) ->
            childPrincipalIds.map { "(${quotePrincipalId(principalId)}, ${quotePrincipalId(it)})" }
        }.joinToString()

        val aclKeyClause = aclKeys?.let {
            val aclKeyArrays = it.joinToString { aclKey -> "'{${aclKey.joinToString()}}'" }

            "WHERE ${PERMISSIONS.name}.${ACL_KEY} IN ($aclKeyArrays)"
        } ?: ""

        return """
            $PRINCIPAL_TREES_CTE (principal_id, child) AS ( VALUES $pairs ),
            $PRINCIPAL_PERMISSIONS AS (
              SELECT
                $PRINCIPAL_TREES_CTE.principal_id,
                ${ACL_KEY.name},
                array_cat_agg(${PostgresColumn.PERMISSIONS.name}) AS ${PostgresColumn.PERMISSIONS.name}
               FROM $PRINCIPAL_TREES_CTE INNER JOIN ${PERMISSIONS.name}
               ON $PRINCIPAL_TREES_CTE.child::text = ${PERMISSIONS.name}.${PRINCIPAL_ID.name}
               $aclKeyClause
               GROUP BY $PRINCIPAL_TREES_CTE.principal_id, ${PERMISSIONS.name}.${ACL_KEY.name}
            )
        """.trimIndent()
    }

    private fun removedPrincipalTreePermissionsCTE(removedPrincipalTree: Pair<String, Set<String>>, aclKeys: Set<AclKey>?): String {
        val principalIdArr = (removedPrincipalTree.second + removedPrincipalTree.first).joinToString { quote(it) }

        val aclKeyClause = aclKeys?.let {
            val aclKeyArrays = it.joinToString { aclKey -> "'{${aclKey.joinToString()}}'" }

            "AND ${PERMISSIONS.name}.${ACL_KEY} IN ($aclKeyArrays)"
        } ?: ""

        return """
            $REMOVED_PRINCIPAL_TREE_PERMS AS (
              SELECT
                ${ACL_KEY.name},
                array_cat_agg(${PostgresColumn.PERMISSIONS.name}) AS ${PostgresColumn.PERMISSIONS.name}
              FROM ${PERMISSIONS.name}
              WHERE
                ${PRINCIPAL_ID.name} = ANY('{$principalIdArr}')
                $aclKeyClause
              GROUP BY ${ACL_KEY.name}
            )
        """.trimIndent()
    }

    // principal_id | acl_key | permissions (to remove)
    private fun permissionDiffSql(
            affectedPrincipalIds: Map<String, Set<String>>,
            removedPrincipalTree: Pair<String, Set<String>>,
            aclKeys: Set<AclKey>? = null
    ): String {

        // principal_id (only for users)
        val principalIdsCTE = principalIdsCTE(affectedPrincipalIds)

        // principal_id | acl_key | permissions
        val principalPermissionsCTE = principalPermissionsCTE(affectedPrincipalIds, aclKeys)

        // acl_key, permissions
        val removedPrincipalTreeCTE = removedPrincipalTreePermissionsCTE(removedPrincipalTree, aclKeys)

        // principal_id | acl_key | permissions
        val permissionsInQuestionCTE = """
            $PERMISSIONS_IN_QUESTION AS ( SELECT * FROM $PRINCIPAL_IDS CROSS JOIN $REMOVED_PRINCIPAL_TREE_PERMS )
        """.trimIndent()

        val permissionDiffClause = """
            SELECT array_agg(p)
            FROM UNNEST($PERMISSIONS_IN_QUESTION.${PostgresColumn.PERMISSIONS.name}) AS perms1(p)
            WHERE NOT EXISTS (
              SELECT 1 FROM UNNEST($PRINCIPAL_PERMISSIONS.${PostgresColumn.PERMISSIONS.name}) as perms2(p)
              WHERE perms1.p = perms2.p
            )
        """.trimIndent()

        return """
            WITH $principalIdsCTE,
                 $principalPermissionsCTE,
                 $removedPrincipalTreeCTE,
                 $permissionsInQuestionCTE
            SELECT
              $PERMISSIONS_IN_QUESTION.${PRINCIPAL_ID.name},
              $PERMISSIONS_IN_QUESTION.${ACL_KEY.name},
              ( $permissionDiffClause ) AS ${PostgresColumn.PERMISSIONS.name}
            FROM
              $PERMISSIONS_IN_QUESTION LEFT JOIN $PRINCIPAL_PERMISSIONS
            ON
              $PERMISSIONS_IN_QUESTION.acl_key = $PRINCIPAL_PERMISSIONS.acl_key
              AND $PERMISSIONS_IN_QUESTION.principal_id::text = $PRINCIPAL_PERMISSIONS.principal_id::text
            WHERE (
              $PRINCIPAL_PERMISSIONS.${PostgresColumn.PERMISSIONS.name} IS NULL
              OR NOT( $PERMISSIONS_IN_QUESTION.${PostgresColumn.PERMISSIONS.name} <@ $PRINCIPAL_PERMISSIONS.${PostgresColumn.PERMISSIONS.name} )
            )
        """.trimIndent()
    }

    private fun initializePostgresFunction() {
        hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(CREATE_AGG_SQL)
            }
        }
    }


}