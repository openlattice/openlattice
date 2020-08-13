package transforms

import com.openlattice.shuttle.transformations.Transformation
import java.util.*

/**
 * Represents a transform that decodes a base64 encoded string
 *
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class Base64DecodeTransform: Transformation<MutableMap<String, String>>(
){
    companion object {
        @JvmStatic
        val decoder = Base64.getDecoder()
    }

    override fun applyValue(s: String?): Any {
        if ( s == null ) {
            return ByteArray(0)
        }
        return decoder.decode(s)
    }
}