import os
from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), "README.rst")) as f:
    readme = f.read().splitlines()

classifiers = """
Development Status :: 3 - Alpha
License :: OSI Approved :: BSD License
Programming Language :: Python :: 2.7
Topic :: Database
"""

setup(
    name='replisome',
    packages=['replisome', 'replisome.consumers', 'replisome.receivers'],
    version='0.0.1',
    description=readme[0],
    long_description='\n'.join(readme[2:]).lstrip(),
    author='Daniele Varrazzo',
    author_email='daniele.varrazzo@gmail.com',
    url='https://github.com/dvarrazzo/wal2json/tree/replisome',
    keywords=['database', 'replication', 'PostgreSQL'],
    classifiers=[x for x in classifiers.strip().splitlines()],
    install_requires=['psycopg2>=2.7'],
    tests_require=['pytest'],
    zip_safe=False,
)
