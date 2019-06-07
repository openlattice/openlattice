package com.openlattice.analysis.requests

import java.time.OffsetDateTime

class WrittenTwoWeeksFilter: AbstractRangeFilter<OffsetDateTime>( OffsetDateTime.now().minusDays(14), true, OffsetDateTime.now().plusYears(100L), true) {
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
