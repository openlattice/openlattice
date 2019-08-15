/*
 * Copyright (C) 2019. OpenLattice, Inc.
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
package com.openlattice.organizations

import com.openlattice.authorization.SecurablePrincipal

class SecurablePrincipalList(val securablePrincipals: MutableList<SecurablePrincipal>)
    : MutableList<SecurablePrincipal> {

    constructor(securablePrincipals: Collection<SecurablePrincipal>) : this(securablePrincipals.toMutableList())

    override val size: Int get() = securablePrincipals.size

    override fun contains(element: SecurablePrincipal): Boolean {
        return securablePrincipals.contains(element)
    }

    override fun containsAll(elements: Collection<SecurablePrincipal>): Boolean {
        return securablePrincipals.containsAll(elements)
    }

    override fun get(index: Int): SecurablePrincipal {
        return securablePrincipals[index]
    }

    override fun indexOf(element: SecurablePrincipal): Int {
        return securablePrincipals.indexOf(element)
    }

    override fun isEmpty(): Boolean {
        return securablePrincipals.isEmpty()
    }

    override fun iterator(): MutableIterator<SecurablePrincipal> {
        return securablePrincipals.iterator()
    }

    override fun lastIndexOf(element: SecurablePrincipal): Int {
        return securablePrincipals.lastIndexOf(element)
    }

    override fun add(element: SecurablePrincipal): Boolean {
        return securablePrincipals.add(element)
    }

    override fun add(index: Int, element: SecurablePrincipal) {
        return securablePrincipals.add(index, element)
    }

    override fun addAll(index: Int, elements: Collection<SecurablePrincipal>): Boolean {
        return securablePrincipals.addAll(index, elements)
    }

    override fun addAll(elements: Collection<SecurablePrincipal>): Boolean {
        return securablePrincipals.addAll(elements)
    }

    override fun clear() {
        return securablePrincipals.clear()
    }

    override fun listIterator(): MutableListIterator<SecurablePrincipal> {
        return securablePrincipals.listIterator()
    }

    override fun listIterator(index: Int): MutableListIterator<SecurablePrincipal> {
        return securablePrincipals.listIterator(index)
    }

    override fun remove(element: SecurablePrincipal): Boolean {
        return securablePrincipals.remove(element)
    }

    override fun removeAll(elements: Collection<SecurablePrincipal>): Boolean {
        return securablePrincipals.removeAll(elements)
    }

    override fun removeAt(index: Int): SecurablePrincipal {
        return securablePrincipals.removeAt(index)
    }

    override fun retainAll(elements: Collection<SecurablePrincipal>): Boolean {
        return securablePrincipals.retainAll(elements)
    }

    override fun set(index: Int, element: SecurablePrincipal): SecurablePrincipal {
        return securablePrincipals.set(index, element)
    }

    override fun subList(fromIndex: Int, toIndex: Int): MutableList<SecurablePrincipal> {
        return securablePrincipals.subList(fromIndex, toIndex)
    }

    override fun equals(other: Any?): Boolean {
        return securablePrincipals == other
    }

    override fun hashCode(): Int {
        return securablePrincipals.hashCode()
    }
}