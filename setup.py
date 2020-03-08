import setuptools


with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="noobWS",
    version="0.0.1",
    author="Lance Chua",
    author_email="lancerobinchua@gmail.com",
    description="Websocket for noobs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lancechua/noobWS",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        "pyzmq>=18.1.0",
        "websockets==8.1",
    ]
)