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

package com.openlattice.mail.utils;

import java.io.IOException;
import java.io.StringReader;
import java.net.URL;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.samskivert.mustache.Mustache;

public final class TemplateUtils {

    public static final Mustache.Compiler DEFAULT_TEMPLATE_COMPILER = Mustache.compiler()
            .withLoader( templateResourcePath -> {
                return new StringReader( loadTemplate( templateResourcePath ) );
            } );

    private TemplateUtils() {}

    public static String loadTemplate( String templatePath ) throws IOException {
        URL templateResource = Resources.getResource( templatePath );
        return Resources.toString( templateResource, Charsets.UTF_8 );
    }

}
