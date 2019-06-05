package com.openlattice.apps.processors;

import com.openlattice.apps.App;
import com.openlattice.apps.AppConfigKey;
import com.openlattice.apps.AppTypeSetting;
import com.openlattice.authorization.Permission;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

import java.util.*;

public class UpdateAppRolePermissionsProcessor extends
        AbstractRhizomeEntryProcessor<UUID, App, Object> {
    private static final long serialVersionUID = -7704850492163706257L;

    private UUID                                            roleId;
    private Map<Permission, Map<UUID, Optional<Set<UUID>>>> permissions;

    public UpdateAppRolePermissionsProcessor( UUID roleId, Map<Permission, Map<UUID, Optional<Set<UUID>>>> permissions ) {
        this.roleId = roleId;
        this.permissions = permissions;
    }

    public UUID getRoleId() {
        return roleId;
    }

    public Map<Permission, Map<UUID, Optional<Set<UUID>>>> getPermissions() {
        return permissions;
    }

    @Override public Object process( Map.Entry<UUID, App> entry ) {
        App app = entry.getValue();
        if ( app != null ) {
            app.setRolePermissions( roleId, permissions );
            entry.setValue( app );
        }
        return null;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        UpdateAppRolePermissionsProcessor that = (UpdateAppRolePermissionsProcessor) o;
        return Objects.equals( roleId, that.roleId ) &&
                Objects.equals( permissions, that.permissions );
    }

    @Override public int hashCode() {
        return Objects.hash( roleId, permissions );
    }
}
