# to generate the python cli easily:

docker run --rm -v ${PWD}:/local openapitools/openapi-generator-cli generate \
    -i /local/openlattice.yaml \
    -g python \
    -o /local/out/python

docker run -p 80:8080 swaggerapi/swagger-ui
