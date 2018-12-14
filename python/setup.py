
from setuptools import setup, find_packages

import stryke

setup(
    name='stryke',
    version=stryke.__version__,
    packages=find_packages(),
    author='Erwan BERNARD',
    author_email='bernard.erwan@gmail.com',
    description="Library to write Orc file",
    ong_description=open('../README.md').read(),
    package_data={'stryke': ['stryke.so']},
    include_package_data=True,
    url='https://github.com/edmBernard/Stryke',
    classifiers=[
        "Programming Language :: Python",
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved",
        "Natural Language :: French",
        "Natural Language :: English",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: C++",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering :: Artificial Intelligence"
    ],
    zip_safe=False,
    license="Apache-2.0"
)
