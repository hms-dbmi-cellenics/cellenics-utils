biomage-utils
=============

Your one-stop shop for managing Biomage infrastructure. This is a Python CLI
application you can use to manage common tasks related to Biomage
infrastructure.

Setup
-----

After cloning the repository, do the following:

    python3 -m venv venv
    source venv/bin/activate
    pip3 install -r requirements.txt

You should be able to access `biomage-utils` by typing:

    python3 biomage

As a prerequisite for running all scripts in this repo, you will need a GitHub Personal Access
Token with full access to your account. You can generate one
[here](https://github.com/settings/tokens). Make sure you note down this and
supply it when required. Utilities can accept this token in two ways:

* either by having it available as the environment variable `GITHUB_API_TOKEN`
* or by passing it as an option with the `-t` flag.

For example:

    GITHUB_API_TOKEN=mytoken python3 biomage stage

or

    python3 biomage stage -t mytoken

Using the environment variable means you can put the token in your
`.bashrc` or `.zshrc` file, thereby avoiding typing it again and again. You can
then simply do:

    python3 biomage stage

### Other Enviroment Variables

* `BIOMAGE_NICK` is optional and used to override the `USER` environment variable
  as the first part of the name of the staging environments created by you:
  `${BIOMAGE_NICK:-${USER}}-...`.

Utilities
---------

### configure-repo

Configures a repository using best practices. You can supply a repository name
as in:

    python3 biomage configure-repo ui

The script will ensure the repository is configured according to the current
best practices for the repository. You can see more details about the
configuration in configure-repo/configure_repo.py script.

### rotate-ci

Rotates the AWS access keys used by the CI runners. You must have the required
AWS rights to use this utility. Credentials will be fetched in the same way as
they are for the AWS CLI.

You can run:

    python3 biomage rotate-ci

and the script should take care of the rest.

### stage

Deploys a staging environment. This utility takes a list of *deployments* as
arguments. A *deployment* can be one of the following:

* A repository name that publishes a staging candidate file to the `iac` repo, e.g. `ui`.
In this case, the manifest fetched is the one for the `master` branch of the `ui` repository.
* A repository name and a pull request ID, e.g. `ui/12`. In this case, the manifest fetched
is the one for the pull request 12 branch of the `ui` repository.

The default deployments for all `stage` commands is `ui`, `api`, `worker`. If you wish to
deploy a different version of these, you can specify that manually. Then, at the bare minimum,
you can run:

    python3 biomage stage

If you wish to test changes to you made to the API available under pull request 25, you can run:

    python3 biomage stage api/25

This will compose a *sandbox* comprising `api` as found under pull request `25`, as well as `ui`
and `worker` as found under `master`.

The utility will launch an interactive wizard to guide you through creating your environment.

#### Pinning

Pinning is a feature of the utility. When you pin a deployment, you ensure that no changes made
to the manifest file after the fact will effect the sandbox you are creating. For example, if you
want to make sure your `ui` feature will not be affected by changes to pushes to master in the `api`
repository after you create the sandbox, you have the option to pin `api` to prevent this from happening.

The default behavior is to pin all deployments that source from `master`. This ensures that the sandbox
is immutable, i.e. anything not currently being tested in the sandbox will not change unpredictably
after it's deployed. Deployments that source from a pull request branch are by default not pinned,
as these are commonly the branches that a developer would push features to mid-development.

### unstage

Removes a staging environment. You must specify the sandbox ID of the staging environment deployed
previously from [here](https://github.com/biomage-ltd/iac/tree/master/releases/staging). Then, run

    python3 biomage unstage my-sandbox-id

to remove your deployment.

### experiment

#### experiment compare

Compares experiment settings accros development/staging/production environments. **Note** it needs inframock running in order to work.

    python3 biomage experiment compare my-experiment-id
