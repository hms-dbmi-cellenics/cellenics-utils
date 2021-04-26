import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

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
    python_requires=">=3.6",
    entry_points={
        "console_scripts": ["biomage=biomage:__main__"],
    },
)
