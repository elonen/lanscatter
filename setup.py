import setuptools

try:
    from pyinstaller_setuptools import setup
except ImportError:
    print("WARNING: pyinstaller_setuptools not installed. You won't be able to `./setup.py pyinstaller`")
    from setuptools import setup


with open("README.md", "r") as f:
    long_description = f.read()

with open('requirements.cli.txt') as f:
    install_requires = f.read()


setup(
    name='lanscatter',

    entry_points={
        'console_scripts': [
            'lanscatter_master = lanscatter.masternode:main',
            'lanscatter_peer = lanscatter.peernode:main',
            'lanscatter_gui = lanscatter.gui:main',
        ],
    },
    data_files=[('bitmaps', ['icon.png', 'icon__bgr.png', 'icon__wheel.png'])],

    version="0.1.4",
    author="Jarno Elonen",
    author_email="elonen@iki.fi",
    description="P2P assisted large file distribution system for modern LAN environments",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/elonen/lanscatter",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    platforms='any',
    install_requires=install_requires
)
