from setuptools import setup, find_packages

setup(
    name='slackutils',
    version='0.1',
    description='Set of utils for Slack',
    long_description=open('README.md').read(),
    url='http://naveler.com:7200/naveler/naveutils',
    author='Naveler',
    email='nestor@naveler.com',
    packages=find_packages(),
    install_requires=[
        'requests',
        'arrow',
        'slackclient',
    ]
)
