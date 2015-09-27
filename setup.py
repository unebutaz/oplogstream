import os
from setuptools import setup, find_packages

try:
    README = open(os.path.join(os.path.dirname(__file__), 'README.rst')).read()
except:
    README = """
    """

dist = setup(
    name="oplogstream",
    version="0.0.1",
    author="Sergey Yashchenko",
    author_email="yashenko.s@gmail.com",
    description=("Daemon for monitoring and publishing mongodb oplog."),
    keywords = "mongodb oplog rabbitmq",
    packages=find_packages(),
    install_requires=['pika', 'pymongo', 'bson'],
    long_description=README,
)
