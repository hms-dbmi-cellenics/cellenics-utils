biomage-utils
=============

Your one-stop shop for managing Biomage infrastructure. This is a Python CLI
application you can use to manage common tasks related to Biomage
infrastructure.

Setup
-----

After cloning the repository, do the following (might require **sudo** in some distributions, and `pip` > 21.0.1):

    make install

You can verify that the command has been successfully installed with:

    make test

If the test was successful, you should be able to access `biomage-utils` by typing:

    biomage --help

As a prerequisite for running all scripts in this repo, you will need a GitHub Personal Access
Token with full access to your account. You can generate one
[here](https://github.com/settings/tokens). Make sure you note down this and
supply it when required. Utilities can accept this token in two ways:

* either by having it available as the environment variable `GITHUB_API_TOKEN`
* or by passing it as an option with the `-t` flag.

For example:

    GITHUB_API_TOKEN=mytoken biomage stage

or

    biomage stage -t mytoken

Using the environment variable means you can put the token in your
`.bashrc` or `.zshrc` file, thereby avoiding typing it again and again. You can
then simply do:

    biomage stage

### Other Enviroment Variables

* `BIOMAGE_NICK` is optional and used to override the `USER` environment variable
  as the first part of the name of the staging environments created by you:
  `${BIOMAGE_NICK:-${USER}}-...`.

*  `BIOMAGE_DATA_PATH`: where to get the experiment data to populate inframock's S3 and DynamoDB. It is recommended
to place it outside any other repositories to avoid interactions with git. For example, `export BIOMAGE_DATA_PATH=$HOME/biomage-data` (or next to where your biomage repos live). If this is not set, it will default to `./data`. **Note**: this should be permanently added to your environment (e.g. in `.zshrc`, `.localrc` or similar) because other services like `inframock` or `worker` rely on using the same path.


Utilities
---------

### configure-repo

Configures a repository using best practices. You can supply a repository name
as in:

    biomage configure-repo ui

The script will ensure the repository is configured according to the current
best practices for the repository. You can see more details about the
configuration in configure-repo/configure_repo.py script.

### rotate-ci

Rotates the AWS access keys used by the CI runners. You must have the required
AWS rights to use this utility. Credentials will be fetched in the same way as
they are for the AWS CLI.

You can run:

    biomage rotate-ci

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

    biomage stage

If you wish to test changes to you made to the API available under pull request 25, you can run:

    biomage stage api/25

This will compose a *sandbox* comprising `api` as found under pull request `25`, as well as `ui`
and `worker` as found under `master`.

The utility will launch an interactive wizard to guide you through creating your environment.

#### *isolated staging environments*

The option to create isolated staging environments is provided during the creation of the staging environment. Creating a new isolated staging environment allows you to modify database records and files without causing changes to other staging environments. This also isolates your staging environment from changes made by others.

Isolated staging environments are created by creating new experimentIds using data from existing experiments. Data and records are copied from source tables and buckets independently of deployment. Therefore, in the event of deployment failure, data and records may be copied successfully. 

If your deployment fails, **you are recommended to use the same sandbox Id that have failed**. The wizard will detect existing staging environemtns and display created experimentIds. If you decide to use a different sandbox ID, [unstage](#unstage) your environment before staging using a new sandbox ID.

During the creation of isolated staging environments, the following files are copied in these S3 buckets into their staging counterpart:

    biomage-source-production
    processed-matrix-production

Records in the following DynamoDB tables are copied into their staging counterparts:

    experiments-production
    samples-production

These configurations are read from `config.yaml`

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

    biomage unstage my-sandbox-id

to remove your deployment and delete staged environment.

### experiment

Manages experiment's data and configuration. See `biomage experiment --help` for more details.

#### experiment pull

Downloads experiment data and config files from a given environment into `BIOMAGE_DATA_PATH/experiment_id/`. See `biomage experiment pull --help` for more information, parameters and default values.

E.g. to download experiment `e52b39624588791a7889e39c617f669e` data from `production`:

`biomage experiment pull production e52b39624588791a7889e39c617f669e`

#### experiment ls

List available experiments data in a given environment. See `biomage experiment ls --help` for more information, parameters and default values.

Example: list experiment files in `production`:

`biomage experiment ls production`

#### experiment compare

Compares experiment settings accros development/staging/production environments. **Note** it needs inframock running in order to work.

    biomage experiment compare my-experiment-id

### release

Creates new releases from develop branches.

`biomage release` -> creates a new release asking for systems to be released.
`biomage release --all` -> creates a new release for all repos (ui, api, pipeline, worker).

#### experiment copy

Copies one experiment information from a given env to another. See `biomage experiment copy --help` for more details.

`biomage experiment copy --experiment_id=e52b39624588791a7889e39c617f669e --sandbox_id=default`
