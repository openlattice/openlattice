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
package com.openlattice.rehearsal.organization

import com.openlattice.authorization.*
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.organization.Organization
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn
import com.openlattice.rehearsal.SetupTestData
import com.zaxxer.hikari.HikariDataSource
import org.junit.Assert
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.util.*

open class AssemblerTestBase : SetupTestData() {

    /**
     * Add permission to materialize entity set and it's properties to organization principal
     */
    fun grantMaterializePermissions(organization: Organization, entitySet: EntitySet, properties: Set<UUID>) {
        val newPermissions = EnumSet.of(Permission.MATERIALIZE)
        val entitySetAcl = Acl(
                AclKey(entitySet.id),
                setOf(Ace(organization.principal, newPermissions, OffsetDateTime.MAX))
        )
        permissionsApi.updateAcl(AclData(entitySetAcl, Action.ADD))

        // add permissions on properties
        properties.forEach {
            val propertyTypeAcl = Acl(
                    AclKey(entitySet.id, it),
                    setOf(Ace(organization.principal, newPermissions, OffsetDateTime.MAX))
            )
            permissionsApi.updateAcl(AclData(propertyTypeAcl, Action.ADD))
        }
    }

    fun getStringResult(rs: ResultSet, column: String): String {
        return PostgresArrays.getTextArray(rs, column)[0]
    }

    fun checkMaterializedEntitySetColumns(
            organizationDataSource: HikariDataSource,
            entitySet: EntitySet,
            entityType: EntityType = edmApi.getEntityType(entitySet.entityTypeId),
            propertyTypeFqns: List<String> = entityType.properties.map { edmApi.getPropertyType(it).type.fullQualifiedNameAsString }
    ) {
        organizationDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                val rs1 = stmt.executeQuery(TestAssemblerConnectionManager.selectFromEntitySetSql(entitySet.name))
                Assert.assertEquals(PostgresColumn.ENTITY_SET_ID.name, rs1.metaData.getColumnName(1))
                Assert.assertEquals(PostgresColumn.ID.name, rs1.metaData.getColumnName(2))
                Assert.assertEquals(PostgresColumn.ENTITY_KEY_IDS_COL.name, rs1.metaData.getColumnName(3))

                // all columns are there
                (1..rs1.metaData.columnCount).forEach {
                    val columnName = rs1.metaData.getColumnName(it)
                    if (columnName != PostgresColumn.ID.name
                            && columnName != PostgresColumn.ENTITY_SET_ID.name
                            && columnName != PostgresColumn.ENTITY_KEY_IDS_COL.name) {
                        Assert.assertTrue(propertyTypeFqns.contains(columnName))
                    }
                }
            }
        }
    }
}