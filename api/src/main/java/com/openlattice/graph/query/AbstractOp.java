/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.graph.query;

import java.util.Set;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public abstract class AbstractOp implements Op {
    private final Integer      id;
    private final Set<Integer> ops;
    private       boolean      negated;

    public AbstractOp( Integer id, Set<Integer> ops, boolean negated ) {
        this.id = id;
        this.ops = ops;
        this.negated = negated;
    }

    public AbstractOp( Set<Integer> ops, boolean negated ) {
        this( Op.idSource.getAndIncrement(), ops, negated );
    }

    @Override public Integer getId() {
        return id;
    }

    @Override
    public AbstractOp negate() {
        negated = !negated;
        return this;
    }

    @Override
    public Set<Integer> getOps() {
        return ops;
    }

    @Override
    public boolean isNegated() {
        return negated;
    }
}
