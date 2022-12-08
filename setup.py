from setuptools import setup, find_packages
from momo import __version__

setup(
    name = 'momo',
    version=__version__,
    description="A simple, lightweight workflow library for maintaining assets",
    author='Trevor Campbell',
    author_email='trevor.d.campbell@gmail.com',
    url='https://github.com/trevorcampbell/momo/',
    packages=find_packages(),
    install_requires=[
        'Click',
    ],
    entry_points={
        'console_scripts':[
            'momo = momo.cli:cli',
        ],
    },
    platforms='ALL',
)
