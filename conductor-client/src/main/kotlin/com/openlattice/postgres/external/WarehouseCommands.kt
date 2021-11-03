package com.openlattice.postgres.external

import com.openlattice.postgres.external.Schemas

/**
 * @author Andrew Carter andrew@openlattice.com
 */
public interface WarehouseCommands {
    @Override fun createUserInWarehouseSql(dbUser: String, dbUserPassword: String): String
    @Override fun createDbSql(warehouseName: String): String
    @Override fun createSchema(dataSource: HikariDataSource, schema: Schemas)
    @Override fun configureOrganizationUser(organizationId: UUID, dataSource: HikariDataSource)
    @Override fun grantOrgUserPrivilegesOnSchemaSql(schema: Schemas, orgUserId: String): String
    @Override fun setDefaultPrivilegesOnSchemaSql(schema: Schemas, usersSql: String): String
    @Override fun setAdminUserDefaultPrivilegesSql(schema: Schemas, usersSql: String): String
    @Override fun revokeAllSql(warehouseName: String, role: String): String
    @Override fun grantMemberWarehousePermissions(warehouseName: String, userName: String): String
}