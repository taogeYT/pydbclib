import re
import ast
from setuptools import setup, find_packages

_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('pydbclib/__init__.py', 'rb') as f:
    rs = _version_re.search(f.read().decode('utf-8')).group(1)
    version = str(ast.literal_eval(rs))

setup(
    name='pydbclib',
    version=version,
    install_requires=['sqlalchemy>=1.1.14', 'sqlparse', "log4py>=2.0"],
    description='Python Database Connectivity Lib',
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",  
        "Operating System :: OS Independent",  
        "Topic :: Text Processing :: Indexing",
        "Topic :: Utilities",  
        "Topic :: Internet",  
        "Topic :: Software Development :: Libraries :: Python Modules",  
        "Programming Language :: Python",
    ],
    author='yatao',
    url='https://github.com/taogeYT/pydbclib',
    author_email='li_yatao@outlook.com',
    license='MIT',
    packages=find_packages(),
    include_package_data=False,
    zip_safe=True,
)
