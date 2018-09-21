from setuptools import setup, find_packages

setup(
    name='ratescan',
    version='0.0.1',
    description='ratescan analysis for FACT',
    url='https://github.com/fact-project/ratescan',
    author='Jens Buss',
    author_email='jens.buss@tu-dortmund.de',
    license='MIT',
    packages=find_packages(),
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    install_requires=[
        'astropy',
        'click',
        'h5py',
        'matplotlib>=2.0',  # in anaconda
        'numpy',            # in anaconda
        'pandas',           # in anaconda
        'pyfact>=0.16.0',
        'python-dateutil',  # in anaconda
        'pytz',             # in anaconda
        'pyyaml',             # in anaconda
        'tables',           # needs to be installed by pip for some reason
        'tqdm',
    ],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'ratescan_fit_and_plot_single_run = ratescan.executables.fitAndPlotSingleRatescan:main',
        ],
    }
)