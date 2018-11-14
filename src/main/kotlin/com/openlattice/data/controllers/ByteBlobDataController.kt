package com.openlattice.data.controllers

import com.kryptnostic.rhizome.configuration.ConfigurationConstants
import com.openlattice.data.storage.ByteBlobDataManager
import org.springframework.context.annotation.Profile
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import javax.inject.Inject

@RestController
@Profile(ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE)
class ByteBlobDataController {
    @Inject
    private lateinit var byteBlobDataManager: ByteBlobDataManager

    @RequestMapping(
            path = ["tempy-media-storage/{key}"],
            method = [(RequestMethod.GET)]
    )
    fun getObject(@PathVariable("key") s3Key: String) : List<Any> {
        return byteBlobDataManager.getObjects(listOf(s3Key))
    }
}