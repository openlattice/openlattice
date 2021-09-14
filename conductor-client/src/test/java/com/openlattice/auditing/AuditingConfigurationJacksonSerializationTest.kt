package com.openlattice.auditing

import com.openlattice.aws.AwsS3ClientConfiguration
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer
import com.openlattice.serializer.AbstractJacksonYamlSerializationTest
import org.junit.BeforeClass
import java.util.*

/**
 *
 *
 */

class AuditingConfigurationJacksonSerializationTest : AbstractJacksonYamlSerializationTest<AuditingConfiguration>() {
    companion object {
        //        val logger = LoggerFactory.getLogger(AuditingConfigurationJacksonSerializationTest::class.java)
        @JvmStatic
        @BeforeClass
        fun setupSerializer() {
            FullQualifiedNameJacksonSerializer.registerWithMapper(yaml)
        }
    }

    override fun getSampleData(): AuditingConfiguration {
        FullQualifiedNameJacksonSerializer.registerWithMapper(yaml)
        return AuditingConfiguration(
                "ol.audit",
                "ol.audited",
                mapOf(AuditProperty.ID to "ol.id"),
                Optional.of(AwsS3ClientConfiguration("blah", "us-gov-east-1", "blah", "blah"))
        )
    }

    override fun logResult(result: SerializationResult<AuditingConfiguration>?) {
        logger.info("Auditing configuration: {}", result?.jsonString)
    }

    override fun getClazz(): Class<AuditingConfiguration> {
        return AuditingConfiguration::class.java
    }

}