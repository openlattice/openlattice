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

package com.openlattice.edm;

import com.openlattice.authorization.HzAuthzTest;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.postgres.DataTables;
import com.openlattice.postgres.PostgresTableManager;
import java.sql.SQLException;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class PostgresEdmTests extends HzAuthzTest {

    @Test
    public void testEntitySetTableRegistration() throws SQLException {
        PostgresTableManager ptm = testServer.getContext().getBean( PostgresTableManager.class );
        EntitySet es = TestDataFactory.entitySet();

    }
}
