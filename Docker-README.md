Overview

This is an attempt to document our work dockerizing the stack

The fundamental structure is the same as our architecture although postgres and elasticsearch will not be containerized as they do not run particularly well inside of containers given their resource needs

The usage process is as follows:

- The docker-compose file (for better or worse) relies on having a locally built version of each service in their respective build/distributions/ folders
build and dist our software packages.
	- I prefer this for building:
		- `gradle -q --build-cache --daemon --parallel build -x test -x spotbugsMain -x spotbugsTest -x checkstyleMain -x checkstyleTest -x javadoc -x distZip -x distTar`
	- and this for creating dists:
		- `gradle -q --build-cache --daemon --parallel :conductor:distTar :datastore:distTar :indexer:distTar -x test -x spotbugsMain -x spotbugsTest -x checkstyleMain -x checkstyleTest -x javadoc -x distZip`
	- run `docker-compose up --build` to start the stack after building
	- run `docker-compose down` to stop the stack


Architecture:

One docker image per service, all of them represented in a docker-compose file to spin up a full stack in one command.

The external dependencies are:
- an elasticsearch cluster
- an openlattice postgres database for external database metadata
- an openlattice postgres database for internal metadata and data
	- both postgres databases can be in the same cluster

The configuration files used to run the stack live in a top level directory layout as follows:
- secrets/
- shared/
- conductor/
- datastore/
- indexer/
- linker/

- The respective service's directory is mounted first, then the shared directory files are mounted over top of their service specific versions

- There are several dockerfiles added to the repository, one per service

- The IP address/host running postgres/elasticsearch must be manually specified in the configuration files as there is no operating-system agnostic placeholder that docker can interpret as 'the host running docker' (your local machine)

- Services have health checks configured and exposed to docker's healthy host monitoring system so if it seems like something isn't starting up and it should be, make sure that all other container dependencies are starting up and becoming healthy
- You may need to increase the compose healthcheck timeouts for ^


Work still to do:

- Openlattice services release process for publishing final versions of each service's latest distributable
	- updating docker-compose to pull from a public repository of microservice dists instead of relying only on locally building our software
- deploy process that publishes all of the docker images to dockerhub 
	- [Publishing a docker image](https://www.tutorialspoint.com/publishing-a-docker-image-on-dockerhub)
- verifying docker-compose is correctly starting the services. Everything should be in place
- currently, its just a matter of iterating by restarting over and over and running tests to make sure everything is responding correctly