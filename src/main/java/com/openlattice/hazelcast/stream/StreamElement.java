package com.openlattice.hazelcast.stream;

import java.util.NoSuchElementException;
import javax.annotation.Nullable;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class StreamElement<T> {
    private static final StreamElement EOF = new StreamElement( true, null );
    private final T       elem;
    private final boolean eof;

    protected StreamElement( @Nullable T elem ) {
        this( false, elem );
    }

    private StreamElement( boolean eof, T elem ) {
        this.elem = elem;
        this.eof = eof;
    }

    public boolean isEof() {
        return eof;
    }

    public T get() {
        if ( EOF == this ) {
            throw new NoSuchElementException( "Unable to get element corresponding to end of stream." );
        }
        return elem;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof StreamElement ) ) { return false; }

        StreamElement<?> that = (StreamElement<?>) o;

        if ( eof != that.eof ) { return false; }
        return elem.equals( that.elem );
    }

    @Override public int hashCode() {
        int result = elem.hashCode();
        result = 31 * result + ( eof ? 1 : 0 );
        return result;
    }

    public static StreamElement eof() {
        return EOF;
    }
}
