package com.openlattice.analysis.requests

import java.time.OffsetDateTime

class WrittenTwoWeeksFilter: AbstractRangeFilter<OffsetDateTime>( OffsetDateTime.now().minusDays(14), true, OffsetDateTime.MAX, true) {
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

}
