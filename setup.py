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
            'ratescan_convert_json_to_hdf5_cluster = ratescan.executables.convertRatescansToHDF5_cluster:main',
            'ratescan_extract_features_per_run = ratescan.executables.extractFeaturesPerRun:main',
            'ratescan_extract_per_run = ratescan.executables.extractRatescansPerRun:main',
            'ratescan_concat_runs = ratescan.executables.concatRatescans:main',
            'ratescan_determine_sw_trigger_event_list = ratescan.executables.determine_sw_trigger_event_list:main',
            'ratescan_applyTrigger = ratescan.executables.applyTrigger:main',
        ],
    }
)