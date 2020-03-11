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

import com.auth0.json.mgmt.jobs.Job
import com.auth0.net.CustomRequest
import com.fasterxml.jackson.core.type.TypeReference
import okhttp3.*
import org.slf4j.LoggerFactory


class UserExportEntity(private val client: OkHttpClient, private val baseUrl: HttpUrl, private val apiToken: String) {

    companion object {
        private val logger = LoggerFactory.getLogger(UserExportJobRequest::class.java)
    }

    /**
     * Submits a user export job to auth0.
     */
    fun submitExportJob(exportJob: UserExportJobRequest): Job {
        val url = baseUrl
                .newBuilder()
                .addPathSegments("api/v2/jobs/users-exports")
                .build()
                .toString()

        val request = CustomRequest(client, url, "POST", object : TypeReference<Job>() {})
                .setBody(exportJob)
                .addHeader("Authorization", "Bearer $apiToken")

        try {
            return request.execute()
        } catch (ex: Exception) {
            logger.info("Encountered exception $ex when submitting export job $exportJob to url $url.")
            throw ex
        }
    }

    /**
     * Retrieves a job result from auth0 by [jobId].
     */
    fun getJob(jobId: String): UserExportJobResult {
        val url = baseUrl
                .newBuilder()
                .addPathSegments("api/v2/jobs/$jobId")
                .build()
                .toString()

        val request = CustomRequest(client, url, "GET", object : TypeReference<UserExportJobResult>() {})
                .addHeader("Authorization", "Bearer $apiToken")

        try {
            return request.execute()
        } catch (ex: Exception) {
            logger.info("Encountered exception $ex when trying to get export job from url $url.")
            throw ex
        }
    }
}