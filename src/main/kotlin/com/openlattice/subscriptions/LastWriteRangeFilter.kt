package com.openlattice.subscriptions

import com.openlattice.analysis.requests.AbstractRangeFilter
import java.time.OffsetDateTime

class LastWriteRangeFilter( lastNotify : OffsetDateTime ): AbstractRangeFilter<OffsetDateTime>(lastNotify, true, OffsetDateTime.now().plusYears(100L), true) {
    override fun getLowerboundSql(): String {
        return "'$lowerbound'"
    }

    override fun getUpperboundSql(): String {
        return "'$upperbound'"
    }

    override fun getMaxValue(): OffsetDateTime {
        return OffsetDateTime.MAX
    }

    override fun getMinValue(): OffsetDateTime {
        return OffsetDateTime.MIN
    }

    override fun asSql(field: String?): String {
        return super.asSql("last_write")
    }
}
