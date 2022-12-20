from setuptools import find_packages, setup

with open("README.md") as f:
    long_description = f.read()

with open("requirements.txt") as f:
    requirements = f.readlines()

with open("dev-requirements.txt") as f:
    dev_requirements = f.readlines()

setup(
    name="cellenics-utils",
    version="0.0.1",

    description="A CLI package for managing Cellenics infrastructure and codebase.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hms-dbmi-cellenics/cellenics-utils",

    packages=find_packages(),
    package_data={
            "": ["config.yaml"],
    },
    entry_points={
        "console_scripts": ["cellenics = cellenics.__main__:main"],
    },
    python_requires=">=3.7",
    install_requires=requirements,
    extras_require={
        "dev": dev_requirements,
    },
)
