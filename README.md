biomage-utils
=============

Your one-stop shop for managing Biomage infrastructure. This is a Python CLI
application you can use to manage common tasks related to Biomage
infrastructure.

Setup
-----

After cloning the repository, do the following:

    make install

If you are going to be developing `biomage-utils` please install also the development dependencies with:

    make develop

You can verify that the command has been successfully installed with:

    make test

If the test was successful, you should be able to access `biomage-utils` by typing:

    biomage --help

As a prerequisite for running all scripts in this repo, you will need a GitHub Personal Access
Token with full access to your account. This token should be given ALL scopes available. You can
generate one
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

### Other Environment Variables

* `BIOMAGE_NICK` is optional and used to override the `USER` environment variable
  as the first part of the name of the staging environments created by you:
  `${BIOMAGE_NICK:-${USER}}-...`.

*  `COGNITO_PRODUCTION_POOL` and `COGNITO_STAGING_POOL`: The Cognito pool ids used for user account administration. It is recommended to set this interactively. For example, run `export COGNITO_PRODUCTION_POOL=eu-west-1_BLAH` before running `biomage account ...`.


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

Rotates the AWS access keys used by the CI runners, *with the exception of iac*.
You must have sufficient AWS rights and github access token to use this
utility. Credentials will be fetched in the same way as they are for the AWS
CLI.

You can run:

    biomage rotate-ci

and the script should take care of the rest.

### stage

Deploys a staging environment. This utility takes a list of *deployments* as
arguments. A *deployment* can be one of the following:

* A repository name that publishes a staging candidate file to the `releases` repo, e.g. `ui`.
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
previously from [here](https://github.com/hms-dbmi-cellenics/releases/tree/master/staging). Then, run

    biomage unstage my-sandbox-id

to remove your deployment and delete staged environment.

### experiment

Manages experiment's data and configuration. See `biomage experiment --help` for more details.

#### experiment download

Download files associated with an experiment.

    biomage experiment download -e my-experiment-id -i environment

Currently download of the following files is supported:

- Sample files
- Raw RDS file
- Processed RDS file
- Cell sets file

**Note** this command needs `biomage rds tunnel` running in another tab to work. By default, `biomage rds tunnel` connects to staging. If you want to use production you need to specify it with the `-i` option (`biomage rds tunnel -i production`).

### account
A set of helper commands to aid with managing Cellenics account information (creating user accounts, changing passwords). See `biomage account --help` for more information, parameters and default values. Needs environmental variables `COGNITO_PRODUCTION_POOL` and/or `COGNITO_STAGING_POOL`.

### rds

Includes many rds connection-related mechanisms. See `biomage rds --help` for more details.

In order to run these you will need the following tools installed:

#### installing
[jq](https://stedolan.github.io/jq/)
```brew install jq```

[psql](https://www.postgresql.org/docs/current/app-psql.html)
```brew install postgresql```

[aws ssm cli](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html)
```
curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/mac/sessionmanager-bundle.zip" -o "sessionmanager-bundle.zip"
unzip sessionmanager-bundle.zip
sudo /usr/local/bin/python3.6 sessionmanager-bundle/install -i /usr/local/sessionmanagerplugin -b /usr/local/bin/session-manager-plugin
```

These commands have a --profile (-p) parameter that represents one of the credential sets stored usually in ~/.aws/credentials.
By adding more than one credential set here, you can run, for example, 
biomage rds tunnel -p {first_profile_name} -lp 5432
biomage rds tunnel -p {second_profile_name} -lp 5433

This will establish 2 tunnels that can be used to connect to the dbs in the first and second accounts corresponding to the profiles simultaneously.

#### rds tunnel

Sets up an ssh tunneling/port forwarding session for the rds server in a given environment.

Example: set up an ssh tunnel to one of the staging rds endpoints
    biomage rds tunnel -i staging

#### rds run

Run a command in the database cluster using IAM if necessary.
Important: If you're not trying to connect in development, you'll need to run `biomage rds tunnel -i staging` first in a different terminal.

Example: login into postgre console in staging
    biomage rds run psql

Example: dump the in staging
    biomage rds run psql

See `biomage rds run --help` for more details.

#### rds migrator

Run Knex migration commands for development and staged environments.

Example: Migrate development database (inframock must be running)
    biomage rds migrator

Example: Undo last migration in development (inframock must be running)
    biomage rds migrator -- migrate:down

Example: Migrate database in staged environment
    biomage rds migrator -i staging -s <sandbox_id>

Example: Rollback all migrations in staged environment
    biomage rds migrator -i staging -s <sandbox_id> -- migrate:rollback --all

See `biomage rds migrate --help` for more details.