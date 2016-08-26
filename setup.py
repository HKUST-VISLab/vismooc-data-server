from setuptools import setup

setup(
    name="mathematician",
    version="0.0.1",
    author="HKUST-VISlab",
    author_email="zhutian.chen@outlook.com",
    description="This is the dataserver of vismooc system",
    keywords="vismooc, dataserver, vislab, hkust",
    url="https://github.com/HKUST-VISLab/vismooc-data-server",
    packages=["mathematician"],
    install_requires = [],
    test_suite="tests.get_tests"
)
