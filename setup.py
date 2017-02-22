from setuptools import setup, find_packages

setup(
    name='dynalchemy',
    version='0.0.1',
    description='''Toolkit for declaring and using tables 
        programmatically in your apps''',
    keywords='sqlalchemy dynamic table creation',
    url='https://github.com/scilo7/dynalchemy/',
    author='scilo7',
    packages=find_packages('dynalchemy'),
    package_dir={'dynalchemy': 'dynalchemy'},
    license="MIT License",
    install_requires=['sqlalchemy'],
)