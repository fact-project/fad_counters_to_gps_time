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
        'fad_counters_to_gps_time.production',
        'fad_counters_to_gps_time.production.isdc',
        'fad_counters_to_gps_time.production.ethz',
    ],
    package_data={'photon_stream': 
        [
            'tests/resources/*',
            'production/resources/*'
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
        'tqdm'
    ],
    zip_safe=False,
    entry_points={'console_scripts': [
        'fad_counter_extraction = fad_counters_to_gps_time.production.worker_node_fad_counter_extraction_main:main',
    ]},
)
