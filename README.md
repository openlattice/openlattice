OpenLattice Super Project
==============================

Get started developing faster!

This superproject helps you get started building on the OpenLattice platform by providing a single repository that pulls together all necessary projects so that you can develop and refactor with confidence.

Getting started
==============================

* Ensure your [ssh-keys are generated](https://help.github.com/articles/generating-ssh-keys). Add them to [Github](http://github.com).
* Clone the superproject and all of its submodules:

        git clone ssh://git@github.com/openlattice/openlattice.git --recurse-submodules

* Install and start Postgresql (`brew install postgres` and `brew services start postgres`)
* Install and start ElasticSearch 5.x (`brew install elasticsearch` and `elasticsearch -E cluster.name=openlattice_development` on OS X)
* Install and start Cassandra 3.10 (`brew install cassandra` and `cassandra` on OS X)

Eclipse environment setup
==============================

* From the `openlattice` directory, run the "eclipse" gradle task to set up the appropriate Eclipse project structure

        ./gradlew eclipse

* In Eclipse, click Import -> Existing Projects (make sure you select the "Search for nested projects" option)
    * The root directory is the `openlattice` superproject directory (wherever that might be on your local filesystem)
    * Import all of the selected projects

IntelliJ environment setup
==============================

We've found that using the built-in Import project at the root level works better than trying to use the gradle project generator. You should also enable annotation processing in the IntelliJ settings.

Running locally 
==============================

* Ensure that:
    * You've completed the "Getting started" section above
    * Xcode is installed and you've agreed to the license
    * `JAVA_HOME` is configured properly and JDK 8 is installed
    
# From the `openlattice` superproject directory:

Tools provided
==============================

* **./gradlew** - The superproject's build.gradle sets the 'developmentMode' variable, allowing you to build your projects using projectDependencies

Adding a new project
==============================

We manage subprojects using [git submodules](https://git-scm.com/book/en/v2/Git-Tools-Submodules). See WORKFLOWS.md for some representative workflows.

If a new subproject needs to be added:

* edit **settings.gradle** to add it as a gradle subproject
* create a new branch using `git checkout -b feature/add-<subproject>`
* run `git submodule add ssh://git@github.com/openlattice/<subproject>`
* commit, push, and submit a pull request. `git commit -a -m "Added subproject" && git push --set-upstream origin feature/test` 


