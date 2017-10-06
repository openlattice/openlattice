## Some Representative Workflows

#### Status of all subprojects:

    $ git submodule foreach 'git status'

#### Check out a specific branch across across all projects:

    $ git submodule foreach "git checkout master"
    $ git submodule foreach "git pull || :" 

#### Creating a release 

    $ git checkout -b release/10-1-2017
    $ git push --set-upstream origin feature/test

    Submit a pull request to super project on github