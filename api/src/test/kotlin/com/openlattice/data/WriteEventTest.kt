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
package com.openlattice.data

import org.junit.Assert
import org.junit.Test

class WriteEventTest {

    @Test
    fun testWriteEventNegativeVersion() {
        val we = WriteEvent(-1L, 1)
        Assert.assertEquals(1, we.version)
    }

    @Test
    fun testWriteEventEquality() {
        val we1 = WriteEvent(-1L, 1)
        val we2 = WriteEvent(-1L, 1)

        Assert.assertEquals(we1, we2)
        println(we1)
    }
}