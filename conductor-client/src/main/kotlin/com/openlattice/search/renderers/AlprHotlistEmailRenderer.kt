package com.openlattice.search.renderers

import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.openlattice.mail.RenderableEmailRequest
import com.openlattice.search.requests.PersistentSearch
import jodd.mail.EmailAttachment
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.net.URL
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import javax.imageio.ImageIO

private val logger = LoggerFactory.getLogger(AlprAlertEmailRenderer::class.java)

private const val FROM_EMAIL = "courier@openlattice.com"
private const val TEMPLATE_PATH = "mail/templates/shared/AlprHotlistAlertTemplate.mustache"
private const val MIME_TYPE_PREFIX = "image/"
private const val JPEG = "jpeg"
private const val PNG = "png"

private val READ_TIMESTAMP_FQN = FullQualifiedName("ol.datelogged")

// vehicle details
private val LICENSE_PLATE_FQN = FullQualifiedName("vehicle.licensenumber")
private val LICENSE_PLATE_IMAGE_FQN = FullQualifiedName("ol.licenseplateimage")
private val VEHICLE_IMAGE_FQN = FullQualifiedName("ol.vehicleimage")
private val LOCATION_COORDS_FQN = FullQualifiedName("ol.locationcoordinates")
private val MAKE_FQN = FullQualifiedName("vehicle.make")
private val MODEL_FQN = FullQualifiedName("vehicle.model")
private val YEAR_FQN = FullQualifiedName("vehicle.year")
private val COLOR_FQN = FullQualifiedName("vehicle.color")
private val HIT_TYPE_FQN = FullQualifiedName("ol.description")
private val DESCRIPTION_FQN = FullQualifiedName("ol.description")
private val NAME_FQN = FullQualifiedName("ol.name")
private val ID_FQN = FullQualifiedName("ol.id")
private val AGENCY_NAME = FullQualifiedName("ol.agencyname")
private val CAMERA_NAME = FullQualifiedName("ol.resourceid")

// neighbors
private val DEVICE_ENTITY_TYPE_ID = UUID.fromString("6b513215-2566-491c-9d08-02a282f4123e")
private val DEPT_ENTITY_TYPE_ID = UUID.fromString("e33ad963-60fd-489d-8cdb-9faca522e18a")

class AlprHotlistEmailRenderer {

    companion object {

        private val dateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm a, z").withZone(
                TimeZone.getTimeZone("America/Los_Angeles").toZoneId()
        )

        private fun getFirstValue(entity: Map<FullQualifiedName, Set<Any>>, fqn: FullQualifiedName): String? {
            val values = entity[fqn] ?: emptySet()
            return if (values.isEmpty()) {
                null
            } else values.iterator().next().toString()

        }

        private fun getFirstValue(entity: Map<FullQualifiedName, Set<Any>>, fqns: List<FullQualifiedName>): String? {
            fqns.forEach {
                getFirstValue(entity, it)?.let { v -> return v }
            }
            return null
        }

        private fun getImage(imageUrlPath: String?, fileType: String): ByteArrayOutputStream? {
            if (imageUrlPath == null) {
                return null
            }

            var stream: ByteArrayOutputStream? = null

            try {
                val imageUrl = URL(imageUrlPath)
                val bufferedImage = ImageIO.read(imageUrl)
                val baos = ByteArrayOutputStream()
                ImageIO.write(bufferedImage, fileType, baos)
                baos.flush()
                stream = baos
            } catch (e: Exception) {
                logger.error("Unable to load image for url {}", imageUrlPath, e)
            }

            return stream

        }

        private fun getMapImage(coords: String?, mapboxToken: String): ByteArrayOutputStream? {
            if (coords == null) {
                return null
            }

            val latAndLon = coords.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            if (latAndLon.size < 2) {
                return null
            }

            val lat = latAndLon[0]
            val lon = latAndLon[1]

            val url = "https://api.mapbox.com/styles/v1/mapbox/streets-v11/static/pin-l-car+000" +
                    "($lon,$lat)/$lon,$lat,15/600x600?access_token=$mapboxToken"

            return getImage(url, PNG)
        }

        private fun getGoogleMapsUrl(coordinates: String?): String? {
            return coordinates?.let {
                val splitStr = coordinates.split(",")
                val lat = splitStr[0]
                val lon = splitStr[1]
                val url = "https://www.google.com/maps/place/$lat,$lon"
                "<a href=\"$url\">$url</a>"
            }

        }

        private fun extractVehicleInfo(vehicleRead: Map<FullQualifiedName, Set<Any>>): Map<String, Any> {

            val values = Maps.newHashMap<String, Any>()

            val readDateTimeList = vehicleRead[READ_TIMESTAMP_FQN] ?: emptySet()

            values["readDateTime"] = if (readDateTimeList.isEmpty()) "" else OffsetDateTime.parse(
                    readDateTimeList.first().toString()
            ).format(dateTimeFormatter)
            values["hitType"] = getFirstValue(vehicleRead, HIT_TYPE_FQN)
            values["make"] = getFirstValue(vehicleRead, MAKE_FQN)
            values["model"] = getFirstValue(vehicleRead, MODEL_FQN)
            values["year"] = getFirstValue(vehicleRead, YEAR_FQN)
            values["color"] = getFirstValue(vehicleRead, COLOR_FQN)
            values["coordinates"] = getGoogleMapsUrl(getFirstValue(vehicleRead, LOCATION_COORDS_FQN))
            values["agencyId"] = getFirstValue(vehicleRead, AGENCY_NAME)
            values["cameraId"] = getFirstValue(vehicleRead, CAMERA_NAME)

            return values.filterValues { it != null }
        }

        private fun extractVehicleImages(vehicleRead: Map<FullQualifiedName, Set<Any>>, mapboxToken: String): List<EmailAttachment<*>> {

            val licenseImageUrl = getFirstValue(vehicleRead, LICENSE_PLATE_IMAGE_FQN)
            val vehicleImageUrl = getFirstValue(vehicleRead, VEHICLE_IMAGE_FQN)
            val coordinates = getFirstValue(vehicleRead, LOCATION_COORDS_FQN)

            val licenseImage = getImage(licenseImageUrl, JPEG)
            val vehicleImage = getImage(vehicleImageUrl, JPEG)
            val mapImage = getMapImage(coordinates, mapboxToken)

            val attachments = Lists.newArrayList<EmailAttachment<*>>()
            if (licenseImage != null) {
                EmailAttachment.with().content(licenseImage.toByteArray(), "$MIME_TYPE_PREFIX$JPEG").name(
                        "license-plate-image"
                ).contentId("plate")
                attachments
                        .add(
                                EmailAttachment.with()
                                        .content(licenseImage.toByteArray(), "$MIME_TYPE_PREFIX$JPEG")
                                        .name("license-plate-image")
                                        .contentId("plate")
                                        .buildByteArrayDataSource()
                        )
            }
            if (vehicleImage != null) {
                attachments.add(
                        EmailAttachment.with()
                                .content(vehicleImage.toByteArray(), "$MIME_TYPE_PREFIX$JPEG")
                                .name("vehicle-image")
                                .contentId("vehicle").buildByteArrayDataSource()
                )
            }
            if (mapImage != null) {
                attachments.add(
                        EmailAttachment.with().content(mapImage.toByteArray(), "$MIME_TYPE_PREFIX$PNG")
                                .name("map-image")
                                .contentId("map").buildByteArrayDataSource()
                )
            }

            return attachments
        }

        fun renderEmail(
                persistentSearch: PersistentSearch,
                vehicle: Map<FullQualifiedName, Set<Any>>,
                userEmail: String,
                mapboxToken: String
        ): RenderableEmailRequest {

            val licensePlate = getFirstValue(vehicle, LICENSE_PLATE_FQN) ?: "[Unknown License Plate]"

            val subject = "New ALPR hotlist read -- $licensePlate"

            val templateObjects: MutableMap<String, Any> = mutableMapOf()
            templateObjects.putAll(persistentSearch.alertMetadata)
            templateObjects.putAll(extractVehicleInfo(vehicle))
            templateObjects["licensePlate"] = licensePlate
            templateObjects["expiration"] = persistentSearch.expiration.format(dateTimeFormatter)

            val attachments = extractVehicleImages(vehicle, mapboxToken).toTypedArray()

            templateObjects["subscriber"] = userEmail

            return RenderableEmailRequest(
                    Optional.of(FROM_EMAIL),
                    arrayOf(userEmail) + persistentSearch.additionalEmailAddresses,
                    Optional.empty(),
                    Optional.empty(),
                    TEMPLATE_PATH,
                    Optional.of(subject),
                    Optional.of(templateObjects),
                    Optional.of(attachments),
                    Optional.empty()
            )
        }
    }
}