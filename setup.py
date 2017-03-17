import io
import os
import re

from setuptools import setup


def get_version():
    regex = r"__version__\s=\s\'(?P<version>[\d\.]+?)\'"

    path = ('aioelasticsearch', '__init__.py')

    return re.search(regex, read(*path)).group('version')


def read(*parts):
    filename = os.path.join(os.path.abspath(os.path.dirname(__file__)), *parts)

    with io.open(filename, encoding='utf-8', mode='rt') as fp:
        return fp.read()


setup(
    name='aioelasticsearch',
    version=get_version(),
    author='wikibusiness',
    author_email='osf@wikibusiness.org',
    url='https://github.com/wikibusiness/aioelasticsearch',
    description='aioelasticsearch-py wrapper asyncio',
    long_description=read('README.rst'),
    install_requires=[
        'elasticsearch>=5.0.0,<6.0.0',
        'aiohttp>=1.3.0,<2.0.0',
    ],
    packages=['aioelasticsearch'],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords=['elasticsearch', 'asyncio', 'aiohttp'],
)
