"""Setup script for Alchemist workflow system."""

from setuptools import setup, find_packages
from pathlib import Path

# Read README file
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""

# Read requirements
requirements_file = Path(__file__).parent / "requirements.txt"
requirements = []
if requirements_file.exists():
    with open(requirements_file, 'r') as f:
        requirements = [
            line.strip() for line in f.readlines()
            if line.strip() and not line.startswith('#')
        ]

setup(
    name="alchemist-workflow",
    version="0.1.0",
    author="Chandini Jain",
    author_email="chandini@example.com",
    description="A flexible workflow system for data processing and reasoning",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chandinijain/alchemist_new",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0"
        ],
        "docs": [
            "sphinx>=7.0.0",
            "sphinx-rtd-theme>=1.3.0"
        ],
        "ml": [
            "scikit-learn>=1.3.0",
            "torch>=2.0.0",
            "transformers>=4.20.0"
        ]
    },
    entry_points={
        "console_scripts": [
            "alchemist=alchemist.cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "alchemist": ["config/templates/*.json", "config/templates/*.yaml"],
    },
)