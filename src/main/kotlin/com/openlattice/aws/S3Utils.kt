package com.openlattice.aws

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.openlattice.datastore.configuration.DatastoreConfiguration

fun newS3Client(regionName: String, s3Credentials: AWSCredentials): AmazonS3 {
    val builder = AmazonS3ClientBuilder.standard()
    builder.region = regionName
    builder.credentials = AWSStaticCredentialsProvider(s3Credentials)
    return builder.build()
}

fun newS3Client(s3ClientConfiguration: AwsS3ClientConfiguration): AmazonS3 {
    val s3Credentials = BasicAWSCredentials(
            s3ClientConfiguration.accessKeyId,
            s3ClientConfiguration.secretAccessKey
    )
    return newS3Client(s3ClientConfiguration.regionName, s3Credentials)
}