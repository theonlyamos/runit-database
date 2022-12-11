from importlib.metadata import entry_points
from setuptools import setup, find_packages

VERSION = '0.0.4'

setup(
    name='runit-database',
    version=VERSION,
    author='Amos Amissah',
    author_email='theonlyamos@gmail.com',
    description='Database for runit',
    long_description='Access your runit project database',
    packages=find_packages(),
    include_package_data=True,
    install_requires=['requests','python-dotenv'],
    keywords='python3 runit developer serverless architecture docker',
    project_urls={
        'Source': 'https://github.com/theonlyamos/runit/',
        'Tracker': 'https://github.com/theonlyamos/runit/issues',
    },
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    entry_points={
        # 'console_scripts': [
        #     'runit=runit.cli:main',
        # ],
    }
)
