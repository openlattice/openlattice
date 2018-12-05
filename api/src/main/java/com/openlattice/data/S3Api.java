package com.openlattice.data;

import retrofit2.Call;
import retrofit2.http.*;

public interface S3Api {

    String SERVICE    = "/datastore";
    String CONTROLLER = "/integration";
    String BASE       = SERVICE + CONTROLLER;

    @PUT
    Call<Void> writeToS3(
            @Url String url,
            @Body byte[] data
    );
}
