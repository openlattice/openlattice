package com.openlattice.apps.processors;

import com.openlattice.apps.AppConfigKey;
import com.openlattice.apps.AppTypeSetting;
import com.openlattice.authorization.Permission;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

import java.util.EnumSet;
import java.util.Map;

public class UpdateAppConfigPermissionsProcessor extends
        AbstractRhizomeEntryProcessor<AppConfigKey, AppTypeSetting, Object> {
    private static final long serialVersionUID = -7704850492163706257L;

    private EnumSet<Permission> permissions;

    public UpdateAppConfigPermissionsProcessor( EnumSet<Permission> permissions ) {
        this.permissions = permissions;
    }

    public EnumSet<Permission> getPermissions() {
        return permissions;
    }

    @Override public Object process( Map.Entry<AppConfigKey, AppTypeSetting> entry ) {
        AppTypeSetting setting = entry.getValue();
        if ( setting != null ) {
            setting.setPermissions( permissions );
            entry.setValue( setting );
        }
        return null;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        UpdateAppConfigPermissionsProcessor that = (UpdateAppConfigPermissionsProcessor) o;

        return permissions != null ? permissions.equals( that.permissions ) : that.permissions == null;
    }

    @Override public int hashCode() {
        return permissions != null ? permissions.hashCode() : 0;
    }
}
