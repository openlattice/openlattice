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

package com.openlattice.web.mediatypes;

import java.io.Serializable;

import org.springframework.http.MediaType;

public class CustomMediaType implements Serializable {

    private static final long     serialVersionUID = -948015479363478194L;

    public static final MediaType TEXT_CSV;

    public static final String    TEXT_CSV_VALUE   = "text/csv;charset=UTF-8";
    
    public static final MediaType TEXT_YAML;
    
    public static final String    TEXT_YAML_VALUE  = "text/x-yaml;charset=UTF-8";

    static {
        TEXT_CSV = MediaType.valueOf( TEXT_CSV_VALUE );
        TEXT_YAML = MediaType.valueOf( TEXT_YAML_VALUE );
    }
}
