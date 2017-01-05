'''Setup script
'''
from setuptools import setup, find_packages

setup(
    name="mathematician",
    version="0.2.1",
    author="HKUST-VISlab",
    author_email="zhutian.chen@outlook.com",
    description="This is the dataserver of vismooc system",
    keywords="vismooc, dataserver, vislab, hkust",
    url="https://github.com/HKUST-VISLab/vismooc-data-server",
    packages=find_packages(),
    install_requires=[
        "aiohttp>=0.22.5",
        "pymongo>=3.3.0"
    ],
    entry_points = {
        'console_scripts': ['vismooc-data-server=mathematician.main:main'],
    },
    test_suite="tests.get_tests"
)
