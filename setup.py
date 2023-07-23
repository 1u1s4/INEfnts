from setuptools import setup, find_packages
setup(
    name='INEfnts',
    version='0.1.1',
    author='Luis Alfredo Alvarado Rodríguez',
    description='ETL para las fuentes del IPC/CBA.',
    long_description='',
    url='https://github.com/1u1s4/INEfnts',
    keywords='development, setup, setuptools',
    python_requires='>=3.7',
    packages=find_packages(),
    py_modules=['Fuentes'],
    install_requires=[
        'pyodbc',
        'pandas',
    ]
)