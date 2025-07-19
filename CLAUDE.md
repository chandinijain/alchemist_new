# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a new Python project in early development stage. The repository currently contains:
- A Python 3.7 virtual environment (`claude-env/`) with minimal dependencies (pip, setuptools)
- Claude Code configuration in `.claude/settings.local.json`

## Development Environment

### Python Environment
- Virtual environment: `claude-env/` (Python 3.7.7)
- Activation: `source claude-env/bin/activate`
- System Python: 3.12.4 available at `/usr/local/bin/python3`

### Current Dependencies
- pip 24.0
- setuptools 41.2.0

## Development Commands

Since this is a new project, standard Python development patterns apply:

### Environment Management
```bash
# Activate virtual environment
source claude-env/bin/activate

# Install new packages
pip install <package-name>

# List installed packages
pip list

# Generate requirements file (when project grows)
pip freeze > requirements.txt
```

### Future Development
As the project grows, consider adding:
- `requirements.txt` or `pyproject.toml` for dependency management
- Test framework (pytest, unittest)
- Code formatting tools (black, flake8)
- Project structure with source code directories

## Project Structure

Currently minimal:
- Root directory contains virtual environment and configuration only
- No source code directories established yet
- Ready for initial project setup and code organization