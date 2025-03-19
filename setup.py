from setuptools import setup, find_packages

REQUIRED_PACKAGES = list(filter(None, list(map(lambda s: s.strip(), open('requirements.txt').readlines()))))

with open("README.md", "r") as readme:
    long_description = readme.read()
setup(
    name='vishwa-fastapi-utils',
    version="0.0.9",
    author="Sai Sharan Tangeda",
    author_email="saisarantangeda@gmail.com",
    description="Base SDK for FastAPI Utils",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vishwa-labs/fastapi-utils",
    install_requires=REQUIRED_PACKAGES,
    include_package_data=True,
    packages=find_packages(),
)
