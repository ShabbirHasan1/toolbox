from distutils.core import setup

setup(
    name="ziprex_toolbox",

    # Version number (initial):
    version="0.1.0",

    # Application author details:
    author="Keang Song",
    author_email="skeang@gmail.com",

    # Packages
    packages=["broker", "data", "bid_ask_stream",
              "zipline_extension", "utils"],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="http://github.com/bernoullio/toolbox",

    #
    # license="LICENSE.txt",
    description="Useful stuff for running forex backtest using zipline",

    long_description=open("README.md").read(),

    # Dependent packages (distributions)
    install_requires=[
        'pandas',
        'zipline',
        'logbook',
        'psycopg2',
        'git+git://github.com/oanda/oandapy@master#egg=oandapy'
    ],
)

