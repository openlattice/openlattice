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
 *
 */

package com.openlattice.hazelcast.serializers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.users.Auth0SyncTask;
import java.io.IOException;

/**
 * Stream
 */
public class Auth0SyncTaskStreamSerializer implements SelfRegisteringStreamSerializer<Auth0SyncTask> {

    @Override public Class<Auth0SyncTask> getClazz() {
        return Auth0SyncTask.class;
    }

    @Override public void write( ObjectDataOutput out, Auth0SyncTask object ) throws IOException {

    }

    @Override public Auth0SyncTask read( ObjectDataInput in ) throws IOException {
        return new Auth0SyncTask();
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.AUTH0_SYNC_TASK.ordinal();
    }

    @Override public void destroy() {

    }
}
