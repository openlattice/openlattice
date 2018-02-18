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

package com.openlattice.mail;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import jodd.mail.att.ByteArrayAttachment;

public class RenderableEmailRequest extends EmailRequest {
    private static final String                   TEMPLATE_PATH_FIELD    = "templatePath";
    private static final String                   SUBJECT_FIELD          = "subject";
    private static final String                   TEMPLATE_OBJS_FIELD    = "templateObjs";
    private static final String                   ATTACHMENTS_FIELD      = "attachments";
    private static final String                   ATTACHMENT_PATHS_FIELD = "attachmentPaths";
    private final Optional<String>                subject;
    private final String                          templatePath;
    private final Optional<Object>                templateObjs;
    private final Optional<ByteArrayAttachment[]> byteArrayAttachment;
    private final Optional<String[]>              attachmentPaths;

    @JsonCreator
    public RenderableEmailRequest(
            @JsonProperty( FROM_FIELD ) Optional<String> from,
            @JsonProperty( TO_FIELD ) String[] to,
            @JsonProperty( CC_FIELD ) Optional<String[]> cc,
            @JsonProperty( BCC_FIELD ) Optional<String[]> bcc,
            @JsonProperty( TEMPLATE_PATH_FIELD ) String templatePath,
            @JsonProperty( SUBJECT_FIELD ) Optional<String> subject,
            @JsonProperty( TEMPLATE_OBJS_FIELD ) Optional<Object> templateObjs,
            @JsonProperty( ATTACHMENTS_FIELD ) Optional<ByteArrayAttachment[]> byteArrayAttachment,
            @JsonProperty( ATTACHMENT_PATHS_FIELD ) Optional<String[]> attachmentPaths ) {
        super( from, to, cc, bcc );
        this.subject = subject;
        this.templatePath = templatePath;
        Preconditions.checkArgument( StringUtils.isNotBlank( this.templatePath ) );
        this.templateObjs = templateObjs;
        this.byteArrayAttachment = byteArrayAttachment;
        this.attachmentPaths = attachmentPaths;
    }

    @JsonProperty( SUBJECT_FIELD )
    public Optional<String> getSubject() {
        return subject;
    }

    @JsonProperty( TEMPLATE_PATH_FIELD )
    public String getTemplatePath() {
        return templatePath;
    }

    @JsonProperty( TEMPLATE_OBJS_FIELD )
    public Optional<Object> getTemplateObjs() {
        return templateObjs;
    }

    @JsonProperty( ATTACHMENT_PATHS_FIELD )
    public Optional<String[]> getAttachmentPaths() {
        return attachmentPaths;
    }

    @JsonProperty( ATTACHMENTS_FIELD )
    public Optional<ByteArrayAttachment[]> getByteArrayAttachment() {
        return byteArrayAttachment;
    }

    public static RenderableEmailRequest fromEmailRequest(
            String subject,
            String templatePath,
            Optional<Object> templateObjs,
            Optional<String[]> attachmentPaths,
            Optional<ByteArrayAttachment[]> byteArrayAttachment,
            EmailRequest request ) {
        return new RenderableEmailRequest(
                request.getFrom(),
                request.getTo(),
                request.getCc(),
                request.getBcc(),
                templatePath,
                Optional.of( subject ),
                templateObjs,
                byteArrayAttachment,
                attachmentPaths );
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ( ( subject == null ) ? 0 : subject.hashCode() );
        result = prime * result + ( ( templatePath == null ) ? 0 : templatePath.hashCode() );
        result = prime * result + ( ( templateObjs == null ) ? 0 : templateObjs.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( !super.equals( obj ) ) return false;
        if ( !( obj instanceof RenderableEmailRequest ) ) return false;
        RenderableEmailRequest other = (RenderableEmailRequest) obj;
        if ( subject == null ) {
            if ( other.subject != null ) return false;
        } else if ( !subject.equals( other.subject ) ) return false;
        if ( templatePath == null ) {
            if ( other.templatePath != null ) return false;
        } else if ( !templatePath.equals( other.templatePath ) ) return false;
        if ( templateObjs == null ) {
            if ( other.templateObjs != null ) return false;
        } else if ( !templateObjs.equals( other.templateObjs ) ) return false;
        return true;
    }

}