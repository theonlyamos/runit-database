from setuptools import setup, find_packages

VERSION = '0.1.1'

with open('README.md', 'rt') as file:
    description = file.read()

setup(
    name='runit-database',
    version=VERSION,
    author='Amos Amissah',
    author_email='theonlyamos@gmail.com',
    description='Database client for runit serverless applications',
    long_description=description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    include_package_data=True,
    python_requires='>=3.10',
    install_requires=[
        'requests>=2.28.0',
        'python-dotenv>=1.0.0',
        'websockets>=11.0',
        'urllib3>=2.0.0',
    ],
    keywords='python3 runit developer serverless architecture database',
    project_urls={
        'Source': 'https://github.com/theonlyamos/runit-database/',
        'Tracker': 'https://github.com/theonlyamos/runit-database/issues',
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
    ],
)
