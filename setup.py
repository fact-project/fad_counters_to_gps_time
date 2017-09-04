from distutils.core import setup

setup(
    name='fad_counters_to_gps_time',
    version='0.0.1',
    description='process the GPS time for each FACT event',
    url='https://github.com/fact-project/',
    author='Dominik, Neise, Sebastian Achim Mueller',
    author_email='neised@phys.ethz.ch, sebmuell@phys.ethz.ch',
    license='MIT',
    install_requires=[
        'docopt',
        'scipy',
        'sklearn',
        'scikit-image',
        'matplotlib',
        'pandas',
        'tqdm',
        'photon_stream',
        'scoop',
        'pyfact>=0.12.1',
        'manure>=0.1.0',
    ],
    zip_safe=False,
    scripts=[
        'bin/fad_counter_production',
        'bin/gps_time_production',
        'bin/single_fad_counter_extraction',
        'bin/single_gps_time_reco',
    ],
)
