package com.openlattice.shuttle.conditions;

import com.openlattice.client.serialization.SerializableFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConditionValueMapper implements SerializableFunction<Map<String, Object>, Object> {
    private final List<Condition> conditions;

    public ConditionValueMapper( List<Condition> conditions ) {
        this.conditions = conditions;
    }

    @Override public Object apply( Map<String, Object> input ) {
        Boolean out = null;
        String schedule = "and";

        for ( Condition t : conditions ) {

            // get conditional logic (and or or)
            if ( t.toString().startsWith( "conditions.ConditionalAnd" ) ) {
                schedule = "and";
            } else if ( t.toString().startsWith( "conditions.ConditionalOr" ) ) {
                schedule = "or";
            } else {
                Boolean temp = (Boolean) t.apply( input );
                if ( schedule == "and" ) {
                    if ( out == null ) {out = temp;} else if ( out ) {out = temp;} else if ( !out ) {}
                } else if ( schedule == "or" ) {
                    if ( out == null ) {out = temp;} else if ( out ) {} else if ( !out ) {out = temp;}
                }
            }
        }
        return out;
    }
}
