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
package com.openlattice.users.export


import com.auth0.json.mgmt.users.User
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.geekbeast.auth0.*
import com.openlattice.data.requests.FileType
import okhttp3.HttpUrl
import java.net.URL
import java.util.*

/**
 * Represents a user export job request POJO used to request a user export job from auth0.
 */
data class UserExportJobRequest(
        private val properties: List<String>,
        @JsonProperty(FORMAT) val format: FileType = FileType.json,
        @JsonProperty(LIMIT) val limit: Int = 10000
) {
    @JsonProperty(FIELDS)
    val fields = properties.map { Field(it) }

    init {
        require(properties.isNotEmpty()) { "At least one user property must be provided for user export." }
    }
}

data class Field(@JsonProperty(NAME) val name: String)

/**
 * Represents a user export job result POJO returned by auth0 when retrieving a user export job.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class UserExportJobResult(
        @JsonProperty(STATUS) val status: String,
        @JsonProperty(FORMAT) val format: FileType,
        @JsonProperty(LOCATION) val location: Optional<String>) {
    init {
        if (status == JobStatus.completed.name) {
            require(location.isPresent) { "Location should be provided if job is completed." }
        }
    }

    fun getDownloadUrl(): URL {
        check(location.isPresent) { "Download location is not present." }
        val httpUrl = HttpUrl.parse(location.get())
                ?: throw IllegalArgumentException("The download location URL cannot be parsed.")

        return httpUrl.url()
    }
}

data class UsersList(val list: List<User>) : List<User> by list

enum class JobStatus {
    completed,
    pending,
    expired
}