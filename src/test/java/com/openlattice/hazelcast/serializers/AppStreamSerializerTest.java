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

package com.openlattice.hazelcast.serializers;

import com.openlattice.apps.App;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.UUID;

public class AppStreamSerializerTest extends AbstractStreamSerializerTest<AppStreamSerializer, App> {
    @Override protected AppStreamSerializer createSerializer() {
        return new AppStreamSerializer();
    }

    @Override protected App createInput() {
        LinkedHashSet<UUID> configIds = new LinkedHashSet<>();
        configIds.add( UUID.randomUUID() );
        configIds.add( UUID.randomUUID() );
        configIds.add( UUID.randomUUID() );
        return new App( UUID.randomUUID(), "name", "title", Optional.of( "description" ), configIds, "https://openlattice.com/app" );
    }
}
