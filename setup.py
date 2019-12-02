from setuptools import setup, find_packages


setup(
    name='fourd',
    version='0.7',
    packages=find_packages(),
    include_package_data=True,
    author="Michele Bertoldi",
    author_email="michele.bertoldi@softwell.it",
    url="https://github.com/mbertoldi/fourd",
    license='BSD',
    classifiers=['Development Status :: 4 - Beta',
                 'License :: OSI Approved :: BSD License',
                 'Intended Audience :: Developers',
                 'Topic :: Database',
         'Programming Language :: Python :: 3'],
    description="Python DB API module for the 4D database",
)

