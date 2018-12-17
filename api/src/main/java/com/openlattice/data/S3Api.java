package com.openlattice.data;

import retrofit2.Call;
import retrofit2.http.*;

/**
 * This is a utility interface to make it easy for clients to write binary data to S3 using a retrofit client.
 * The base URL of the retrofit client utilizing this interfaces must specify the correct url bucket.
 */
public interface S3Api {
    @PUT
    Call<Void> writeToS3(
            @Url String url,
            @Body byte[] data
    );
}
