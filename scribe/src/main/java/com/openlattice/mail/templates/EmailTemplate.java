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
 */

package com.openlattice.mail.templates;

public enum EmailTemplate {
    SERVICE_OUTAGE( "Service Outage", "mail/templates/shared/outage.mustache" ),

    INTERNAL_ERROR( "Internal Error", "mail/templates/shared/error.mustache" );

    private static final String COURIER_EMAIL_ADDRESS = "Courier <courier@openlattice.com>";
    private final String path;
    private final String subject;

    EmailTemplate( String subject, String path ) {
        this.subject = subject;
        this.path = path;
    }

    public String getSubject() {
        return this.subject;
    }

    public String getPath() {
        return this.path;
    }

    public static String getCourierEmailAddress() {
        return COURIER_EMAIL_ADDRESS;
    }

}
