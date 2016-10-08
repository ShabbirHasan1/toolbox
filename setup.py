from distutils.core import setup

setup(
    name="forex_toolbox",

    # Version number (initial):
    version="0.1.0",

    # Application author details:
    author="Keang Song",
    author_email="skeang@gmail.com",

    # Packages
    packages=["forex_toolbox"],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="http://github.com/bernoullio/toolbox",

    description="Useful stuff for running forex backtest using zipline",

    long_description=open("README.md").read(),

    classifiers=[
        'License :: OSI Approved :: MIT License'
    ],

    # Dependent packages (distributions)
    install_requires=[
        'pandas>=0.16',
        'zipline>1.0',
        'logbook',
        'psycopg2',
        'oandapy',
    ],
    dependency_links=[
        'git+git://github.com/oanda/oandapy@master#egg=oandapy'
    ],
)
