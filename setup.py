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
    ],
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
)
