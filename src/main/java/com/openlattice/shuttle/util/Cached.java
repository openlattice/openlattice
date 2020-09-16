package com.openlattice.shuttle.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.jetbrains.annotations.NotNull;

import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Cached {

    private static final LoadingCache<String, Pattern> regexCache = buildRegexCache();
    private static final LoadingCache<String, Pattern> insensitiveRegexCache = buildInsensitiveRegexCache();
    private static final LoadingCache<String, DateTimeFormatter> formatCache = buildFormatCache();

    private static LoadingCache<String, Pattern> buildRegexCache() {
        return CacheBuilder.newBuilder().build( CacheLoader.from( (key) -> Pattern.compile( key ) ) );
    }

    private static LoadingCache<String, Pattern> buildInsensitiveRegexCache() {
        return CacheBuilder.newBuilder()
                .build( CacheLoader.from( (key) -> Pattern.compile( key, Pattern.CASE_INSENSITIVE ) ) );
    }

    private static LoadingCache<String, DateTimeFormatter> buildFormatCache() {
        return CacheBuilder.newBuilder()
                .build( CacheLoader.from( DateTimeFormatter::ofPattern ) );
    }

    public static Matcher getInsensitiveMatcherForString( @NotNull String targetString, @NotNull String regex ) {
        Pattern pat = insensitiveRegexCache.getUnchecked( regex );
        return pat.matcher( targetString );
    }

    public static Matcher getMatcherForString( @NotNull String targetString, @NotNull String regex ) {
        Pattern pat = regexCache.getUnchecked( regex );
        return pat.matcher( targetString );
    }

    public static DateTimeFormatter getDateFormatForString( @NotNull String dateFormatString ) throws ExecutionException {
        return formatCache.get( dateFormatString );
    }
}
