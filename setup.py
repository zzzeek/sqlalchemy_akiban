import os
import re

from setuptools import setup

v = open(os.path.join(os.path.dirname(__file__), 'sqlalchemy_akiban', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), 'README.rst')


setup(name='sqlalchemy_akiban',
      version=VERSION,
      description="Akiban Dialect and ORM Extension for SQLAlchemy",
      long_description=open(readme).read(),
      classifiers=[
      'Development Status :: 3 - Alpha',
      'Environment :: Console',
      'Intended Audience :: Developers',
      'Programming Language :: Python',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: Implementation :: CPython',
      'Programming Language :: Python :: Implementation :: PyPy',
      'Topic :: Database :: Front-Ends',
      ],
      keywords='Akiban SQLAlchemy',
      author='Mike Bayer',
      author_email='mike@zzzcomputing.com',
      license='MIT',
      packages=['sqlalchemy_akiban'],
      include_package_data=True,
      tests_require=['nose >= 0.11'],
      test_suite="nose.collector",
      zip_safe=False,
      entry_points={
         'sqlalchemy.dialects': [
              'akiban = sqlalchemy_akiban.psycopg2:AkibanPsycopg2Dialect',
              'akiban.psycopg2 = sqlalchemy_akiban.psycopg2:AkibanPsycopg2Dialect',
              ]
        }
)
