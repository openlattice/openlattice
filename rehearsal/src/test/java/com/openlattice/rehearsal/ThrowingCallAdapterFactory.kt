package com.openlattice.rehearsal

import com.google.common.base.Charsets
import java.io.IOException
import java.lang.reflect.Type
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import retrofit2.Call
import retrofit2.CallAdapter
import retrofit2.Retrofit

class ThrowingCallAdapterFactory: CallAdapter.Factory() {
    companion object {
        private val logger = LoggerFactory.getLogger( ThrowingCallAdapterFactory::class.java )
    }

    override fun get(returnType: Type, annotations: Array<kotlin.Annotation>, retrofit: Retrofit): CallAdapter<*, *> =
        object : CallAdapter<Any,Any>  {
            override fun  responseType():Type {
                return returnType
            }

            override fun adapt( call:Call<Any> ):Any? {
                try {
                    val response = call.execute()
                    if ( response.code() >= 400 ) {
                        val errorBody = IOUtils.toString( response.errorBody()!!.byteStream(), Charsets.UTF_8 )
                        logger.error( "Call failed with code {} and message {} and error body {}",
                                response.code(),
                                response.message(),
                                errorBody )
                        throw GeneralException( errorBody )
                    }
                    return response.body()
                } catch ( e:IOException ) {
                    logger.error( "Call failed.", e )
                    return null
                }
            }

        }
}

class GeneralException:java.lang.Exception {
    constructor( message:String ): super(message)
}