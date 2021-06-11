import setuptools

with open("README.md") as f:
    long_description = f.read()

setuptools.setup(
    name="biomage-utils",
    version="0.0.1",

    author="Biomage Ltd.",
    author_email="hello@biomage.net",

    description="A CLI package for managing Biomage infrastructure and codebase.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/biomage-ltd/biomage-utils",

    packages=setuptools.find_packages(),
    entry_points={
        "console_scripts": ["biomage = biomage.__main__:main"],
    },

    python_requires=">=3.8",
    install_requires=[
        'click',
        'requests',
        'PyGithub',
        'PyInquirer',
        'boto3',
        'cfn-flip',
        'deepdiff',
        'anybase32',
    ],
    extras_require={
        'dev': [
            'flake8',
            'black',
        ]
    }
)
