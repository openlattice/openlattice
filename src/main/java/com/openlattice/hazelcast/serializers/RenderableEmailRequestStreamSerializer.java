package com.openlattice.hazelcast.serializers;

import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.IoPerformingBiConsumer;
import com.kryptnostic.rhizome.hazelcast.serializers.IoPerformingFunction;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.mail.RenderableEmailRequest;
import jodd.mail.att.ByteArrayAttachment;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class RenderableEmailRequestStreamSerializer implements SelfRegisteringStreamSerializer<RenderableEmailRequest> {

    @Override
    public Class<? extends RenderableEmailRequest> getClazz() {
        return RenderableEmailRequest.class;
    }

    private static <T> void writeOptional(
            ObjectDataOutput out,
            Optional<T> object,
            IoPerformingBiConsumer<ObjectDataOutput, T> c ) throws IOException {
        final boolean isPresent = object.isPresent();
        out.writeBoolean( isPresent );
        if ( isPresent ) {
            c.accept( out, object.get() );
        }
    }

    private static <T> Optional<T> readOptional( ObjectDataInput input, IoPerformingFunction<ObjectDataInput, T> c )
            throws IOException {
        final boolean isPresent = input.readBoolean();
        if ( isPresent ) {
            return Optional.of( c.apply( input ) );
        }

        return Optional.absent();
    }

    @Override
    public void write( ObjectDataOutput out, RenderableEmailRequest object ) throws IOException {
        writeOptional( out, object.getFrom(), ObjectDataOutput::writeUTF );
        out.writeUTFArray( object.getTo() );
        writeOptional( out, object.getCc(), ObjectDataOutput::writeUTFArray );
        writeOptional( out, object.getBcc(), ObjectDataOutput::writeUTFArray );
        out.writeUTF( object.getTemplatePath() );
        writeOptional( out, object.getSubject(), ObjectDataOutput::writeUTF );
        writeOptional( out, object.getTemplateObjs(), ObjectDataOutput::writeObject );
        writeOptional( out, object.getAttachmentPaths(), ObjectDataOutput::writeUTFArray );

        writeOptional( out, object.getByteArrayAttachment(), ( objectDataOutput, byteArrayAttachments ) -> {
            objectDataOutput.writeInt( byteArrayAttachments.length );

            for ( int i = 0; i < byteArrayAttachments.length; i++ ) {
                ByteArrayAttachment attachment = byteArrayAttachments[ i ];
                objectDataOutput.writeUTF( attachment.getContentType() );
                objectDataOutput.writeUTF( attachment.getContentId() );
                objectDataOutput.writeUTF( attachment.getName() );
                objectDataOutput.writeByteArray( attachment.toByteArray() );
            }
        } );

    }

    @Override
    public RenderableEmailRequest read( ObjectDataInput in ) throws IOException {
        Optional<String> from = readOptional( in, ObjectDataInput::readUTF );
        String[] to = in.readUTFArray();
        Optional<String[]> cc = readOptional( in, ObjectDataInput::readUTFArray );
        Optional<String[]> bcc = readOptional( in, ObjectDataInput::readUTFArray );
        String templatePath = in.readUTF();
        Optional<String> subject = readOptional( in, ObjectDataInput::readUTF );
        Optional<Object> templateObjs = readOptional( in, ObjectDataInput::readObject );
        Optional<String[]> attachmentPaths = readOptional( in, ObjectDataInput::readUTFArray );

        Optional<ByteArrayAttachment[]> byteArrayAttachment = readOptional( in, input -> {
            int size = input.readInt();
            ByteArrayAttachment[] attachments = new ByteArrayAttachment[ size ];

            for ( int i = 0; i < size; i++ ) {
                String contentType = input.readUTF();
                String contentId = input.readUTF();
                String name = input.readUTF();
                byte[] content = input.readByteArray();

                attachments[ i ] = new ByteArrayAttachment( content, contentType, name, contentId );
            }

            return attachments;
        } );

        return new RenderableEmailRequest( from,
                to,
                cc,
                bcc,
                templatePath,
                subject,
                templateObjs,
                byteArrayAttachment,
                attachmentPaths );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.RENDERABLE_EMAIL_REQUEST.ordinal();
    }

    @Override
    public void destroy() {
    }
}
