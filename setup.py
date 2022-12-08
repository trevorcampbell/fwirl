from setuptools import setup, find_packages
from fwirl import __version__

setup(
    name = 'fwirl',
    version=__version__,
    description="A simple, lightweight workflow library for maintaining assets",
    author='Trevor Campbell',
    author_email='trevor.d.campbell@gmail.com',
    url='https://github.com/trevorcampbell/fwirl/',
    packages=find_packages(),
    install_requires=[
        'Click',
    ],
    entry_points={
        'console_scripts':[
            'fwirl = fwirl.cli:cli',
        ],
    },
    platforms='ALL',
)
