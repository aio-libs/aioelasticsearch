import io
import re

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def get_version():
    regex = r"""__version__\s+=\s+(?P<quote>['"])(?P<version>.+?)(?P=quote)"""
    with io.open('aioelasticsearch/__ini__.py', mode='rt', encoding='utf-8') as fp:  # noqa
        return re.search(regex, fp.read()).group('version')


def get_long_description():
    with io.open('README.rst', mode='rt', encoding='utf-8') as fp:
        return fp.read()


setup(
    name='aioelasticsearch',
    version=get_version(),
    author='wikibusiness',
    author_email='osf@wikibusiness.org',
    url='https://github.com/wikibusiness/aioelasticsearch',
    description='aioelasticsearch-py wrapper asyncio',
    long_description=get_long_description(),
    install_requires=[
        'elasticsearch>=5',
        'aiohttp>=1.3',
    ],
    extras_require={
        ':python_version=="3.3"': ['asyncio'],
    },
    py_modules=['aioelasticsearch'],
    zip_safe=False,
    platforms='any',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
