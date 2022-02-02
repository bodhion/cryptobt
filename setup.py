from setuptools import setup

setup(
   name='cryptobt',
   version='1.0.1',
   description='Crypto live data feed and trading support for backtrader',
   url='https://github.com/bodhion/cryptobt',
   author='bodhion',
   author_email='crpytobt@bodhion.com',
   license='MIT',
   packages=['cryptobt'],  
   install_requires=['backtrader', 'ccxt', 'tscache'],
)