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

For most deployments to work properly, you will need a GitHub Personal Access
Token with full access to your account. You can generate one
[here](https://github.com/settings/tokens). Make sure you note down this and
supply it when required. Utilities can accept this token in two ways:

* either by having it available as the environment variable `GITHUB_API_TOKEN`
* or by passing it as an option with the `-t` flag.

For example:
    
    GITHUB_API_TOKEN=mytoken python3 biomage stage

or

    python3 biomage -t mytoken stage

Using the environment variable means you can put the token in your
`.bashrc` or `.zshrc` file, thereby avoiding typing it again and again.

Utilities
---------

### configure-repo

