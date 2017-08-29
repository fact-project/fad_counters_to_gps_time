from distutils.core import setup

setup(
    name='fad_counters_to_gps_time',
    version='0.0.1',
    description='process the GPS time for each FACT event',
    url='https://github.com/fact-project/',
    author='Dominik, Neise, Sebastian Achim Mueller',
    author_email='neised@phys.ethz.ch, sebmuell@phys.ethz.ch',
    license='MIT',
    packages=[
        'fad_counters_to_gps_time',
    ],
    package_data={'photon_stream': [
            'tests/resources/*',
        ]
    },
    install_requires=[
        'docopt',
        'scipy',
        'sklearn',
        'scikit-image',
        'matplotlib',
        'pyfact',
        'pandas',
        'tqdm',
        'photon_stream',
        'scoop',
    ],
    zip_safe=False,
    entry_points={'console_scripts': [
        'single_fad_counter_extraction = fad_counters_to_gps_time.fad_counter_readout:main',
        'single_gps_time_reco = fad_counters_to_gps_time.gps_time_reconstruction:main',
        'produce_fad_counters = fad_counters_to_gps_time.fad_counter_production:main',
        'produce_gps_time = fad_counters_to_gps_time.gps_time_production:main',
    ]},
)
