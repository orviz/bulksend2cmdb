from setuptools import setup

setup(
    name='bulksend2cmdb',
    version='1.0.0',
    description='Script for storing CIP data on CMDBv1',
    url='https://github.com/orviz/bulksend2cmdb',
    author='Pablo Orviz',
    author_email='orviz@ifca.unican.es',
    license='Apache 2.0',
    packages=['bulksend2cmdb'],
    package_dir={'bulksend2cmdb':'bulksend2cmdb'},
    install_requires=[
        'requests',
        'simplejson',
        'six',
    ],
    zip_safe=False,
    entry_points ={
        'console_scripts': ['bulksend2cmdb=bulksend2cmdb.main:main']
    }
)
