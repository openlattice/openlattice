package transforms

import com.fasterxml.jackson.annotation.JsonCreator
import com.openlattice.shuttle.transformations.Transformation
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Represents a transform that decodes a base64 encoded string
 *
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class Base64DecodeTransform: Transformation<Map<String, String>>{

    @JsonCreator
    constructor(): super()

    companion object {
        private val logger = LoggerFactory.getLogger(Base64DecodeTransform::class.java)

        @JvmStatic
        private val decoder = Base64.getDecoder()

        @JvmStatic
        private val EMPTY_BYTES = ByteArray(0)

        @JvmStatic
        fun process( s: String? ): ByteArray {
            logger.info("base64 decode happening")
            if ( s.isNullOrBlank() ) {
                return EMPTY_BYTES
            }
            return decoder.decode(s)
        }
    }

    override fun applyValue(s: String?): Any {
        return process(s)
    }
}