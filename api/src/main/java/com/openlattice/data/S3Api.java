package com.openlattice.data;

import okhttp3.RequestBody;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.PUT;
import retrofit2.http.Url;

public interface S3Api {

    String SERVICE    = "/datastore";
    String CONTROLLER = "/integration";
    String BASE       = SERVICE + CONTROLLER;

    @PUT
    Call<Void> writeToS3(
            @Url String url,
            @Body RequestBody data
    );
}
