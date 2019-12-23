import setuptools
import pyinstaller_setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

pyinstaller_setuptools.setup(
    name='lanscatter',

    entry_points={
        'console_scripts': [
            'lanscatter_master = lanscatter.masternode:main',
            'lanscatter_peer = lanscatter.peernode:main',
            'lanscatter_gui = lanscatter.gui:main',
        ],
    },
    data_files=[('bitmaps', ['hmq.png'])],

    version="0.1",
    author="Jarno Elonen",
    author_email="jarno.elonen@housemarque.com",
    description="P2P assisted large file distribution system for modern LAN environments",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/elonennen/lanscatter",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',

)
