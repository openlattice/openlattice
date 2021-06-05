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

import com.auth0.net.TelemetryInterceptor
import okhttp3.HttpUrl
import okhttp3.OkHttpClient

class Auth0ApiExtension(val domain: String, private val apiToken: String) {

    private val baseUrl = HttpUrl.parse("https://$domain")
            ?: throw IllegalArgumentException("Domain '$domain' couldn't be parsed as an URL.")
    private val client = OkHttpClient.Builder()
            .addInterceptor(TelemetryInterceptor())
            .build()


    fun userExport(): UserExportEntity {
        return UserExportEntity(client, baseUrl, apiToken)
    }
}