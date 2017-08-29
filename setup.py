from distutils.core import setup

setup(
    name='fad_counters_to_gps_time',
    version='0.0.0',
    description='process the GPS time for each FACT event',
    url='https://github.com/fact-project/',
    author='Dominik, Neise, Sebastian Achim Mueller',
    author_email='neised@phys.ethz.ch, sebmuell@phys.ethz.ch',
    license='MIT',
    packages=[
        'fad_counters_to_gps_time',
        'fad_counters_to_gps_time.fad_counter_production',
    ],
    package_data={'photon_stream': [
            'tests/resources/*',
            'gps_time_reconstruction/README.md',
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
    ],
    zip_safe=False,
    entry_points={'console_scripts': [
        ('fad_counter_extraction = ' +
            'fad_counters_to_gps_time.' +
            'fad_counter_production.__init__:main'),
        ('gps_time_reconstruction = ' +
            'fad_counters_to_gps_time.' +
            'gps_time_reconstruction.__init__:main'),
        ('fad_counter_status_update = ' +
            'fad_counters_to_gps_time.' +
            'status.status:main'),
        ('produce_gps_time_at_isdc = ' +
            'fad_counters_to_gps_time.' +
            'gps_time_reconstruction.isdc_production:main'),
        ('produce_fad_counter_at_isdc = ' +
            'fad_counters_to_gps_time.' +
            'fad_counter_production.isdc_production:main'),
    ]},
)
