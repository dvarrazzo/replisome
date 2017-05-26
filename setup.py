import os
import re
from setuptools import setup

DIR = os.path.dirname(__file__)

with open(os.path.join(DIR, "README.rst")) as f:
    readme = f.read().splitlines()

with open(os.path.join(DIR, "replisome/version.py")) as f:
    m = re.search(r"""^VERSION\s*=\s*["']([^'"]+)""", f.read(), re.MULTILINE)
    assert m, "version not found"
    version = m.group(1)

classifiers = """
Development Status :: 3 - Alpha
License :: OSI Approved :: BSD License
Programming Language :: Python :: 2.7
Topic :: Database
"""

setup(
    name='replisome',
    packages=['replisome', 'replisome.consumers', 'replisome.receivers'],
    version=version,
    description=readme[0],
    long_description='\n'.join(readme[2:]).lstrip(),
    author='Daniele Varrazzo',
    author_email='daniele.varrazzo@gmail.com',
    url='https://github.com/GambitResearch/replisome',
    keywords=['database', 'replication', 'PostgreSQL'],
    classifiers=[x for x in classifiers.strip().splitlines()],
    install_requires=['PyYAML', 'psycopg2>=2.7'],
    tests_require=['pytest'],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'replisome = replisome.cli:entry_point',
        ],
    },
)
