from setuptools import setup

classifiers = [
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 2",
    "Topic :: Software Development :: Libraries",
]

setup(
    name='bitfinex-manager',
    version='0.0.1',
    py_modules=['bitfinex_manager'],
    url='https://github.com/gitguild/bitfinex-manager',
    license='MIT',
    classifiers=classifiers,
    author='Ira Miller',
    author_email='ira@gitguild.com',
    description='Bitfinex plugin for the trade manager platform.',
    setup_requires=['pytest-runner'],
    install_requires=[
        'sqlalchemy>=1.0.9',
        'trade_manager>=0.0.1'
    ],
    tests_require=['pytest', 'pytest-cov']
)
