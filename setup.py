from distutils.core import setup
from setuptools import find_packages

setup(name='natrix',
      version='0.1',
      description='Natrix framework',
      author='Roman Yepifanov',
      url='https://github.com/yero13/natrix',
      license='MIT',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Environment :: Console',
          'Programming Language :: Python :: 3.6'],
      packages=find_packages(),
      package_data = {'natrix': ['.LICENSE']},
      package_dir={'.':'natrix'},
      python_requires= '~=3.6',
      install_requires=['pandas', 'jsonschema', 'requests', 'pymongo', 'jsondiff']
      )