/*
 * Copyright (C) 2020. OpenLattice, Inc.
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

package com.openlattice.users.export

import com.openlattice.data.requests.FileType
import com.openlattice.serializer.AbstractJacksonSerializationTest
import java.net.URL
import java.util.Optional

class UserExportJobResultSerializationTest : AbstractJacksonSerializationTest<UserExportJobResult>() {
    override fun getSampleData(): UserExportJobResult {
        return UserExportJobResult(
                JobStatus.COMPLETED,
                FileType.json,
                Optional.of(
                        URL("https://user-exports.auth0.com/job_xi2asXGYWQi1MqOY/openlattice.json.gz?Expires=15" +
                                "83961308&Key-Pair-Id=APKAJPL62IJALBDMSSCA&Signature=JJgOST5K8G4wmgLN~yxlyPFAM~Q7a5bZ" +
                                "UF61QvKI12G2GndvX6HWPcVuvkkqzDfLc6viDTkGJfR22SLOFrlY7M6HkqJ4hDiLSEk2HO-wMTGU2nEPfHFc" +
                                "TA4Cf5TuAv5~ORWLeGoNceG~wxfElqXDefeW26vfUk7bBg~a4lWIYikIeBTB-ITigu4krLx4pXhxOlSVXvrZ" +
                                "nFi-7HUW2fXFn2TlyfU7fI0ZJaG8IT7L9Z-j9SpDJAWpzcvTJB6kupTGwBcrH57v8fm7Rmt7VGpCZI3kN-M_"
                        )
                )
        )
    }

    override fun getClazz(): Class<UserExportJobResult> {
        return UserExportJobResult::class.java
    }
}