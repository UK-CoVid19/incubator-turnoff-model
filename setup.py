import setuptools

setuptools.setup(
    name='temperature-alert-pipeline',
    version='0.0.1',
    install_requires=['numpy', 'apache_beam'],
    packages=setuptools.find_packages(),
)