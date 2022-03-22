from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / 'README.rst').read_text(encoding='utf-8')

setup(
    name='nvk_ds',
    version='0.1.0',
    description='Обертка для запросов к различного рода источникам данных',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    url='https://github.com/audrus1917/nvk_ds',
    author='Andrey Kozhevnikov',
    author_email='andreykozhevnikov@novakidschool.com',
    packages=find_packages(),
    python_requires='>=3.7',
    install_requires=[
        'psycopg2-binary>=2.8.4',
        'google-api-python-client>=2.32.0',
        'prefect>=0.15.11',
        'pyarrow==7.0.0',
        'catboost==0.26.1'
    ],
)
