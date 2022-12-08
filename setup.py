from setuptools import setup, find_packages
from sentry import __version__

setup(
    name = 'sentry',
    version=__version__,
    description="A simple, lightweight workflow library for maintaining assets",
    author='Trevor Campbell',
    author_email='trevor.d.campbell@gmail.com',
    url='https://github.com/trevorcampbell/sentry/',
    packages=find_packages(),
    install_requires=[
        'Click',
    ],
    entry_points={
        'console_scripts':[
            'sentry = sentry.cli:cli',
        ],
    },
    platforms='ALL',
)
