from distutils.core import setup
from setuptools import find_packages

setup(name='na3x',
      version='0.1.1',
      description='Na3x data transformation and integration framework',
      author='Roman Yepifanov',
      url='https://github.com/yero13/na3x',
      license='MIT',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Environment :: Console',
          'Programming Language :: Python :: 3.6'],
      packages=find_packages(),
      package_data = {'na3x': ['.LICENSE']},
      package_dir={'.':'na3x'},
      python_requires= '~=3.6',
      install_requires=['pandas', 'jsonschema', 'requests', 'pymongo', 'jsondiff']
      )