package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.notifications.sms.SmsInformationKey
import com.openlattice.notifications.sms.SmsInformationMapstore
import org.apache.commons.lang3.RandomStringUtils
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class SmsInformationKeyStreamSerializerTest: AbstractStreamSerializerTest<SmsInformationKeyStreamSerializer, SmsInformationKey>() {
    override fun createSerializer(): SmsInformationKeyStreamSerializer {
        return SmsInformationKeyStreamSerializer()
    }

    override fun createInput(): SmsInformationKey {
        return SmsInformationKey(RandomStringUtils.randomAlphanumeric(10), UUID.randomUUID())
    }
}