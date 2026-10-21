"""Запуск тестов: python run_tests.py [аргументы]."""
import sys

import pytest

if __name__ == "__main__":
    sys.exit(pytest.main(sys.argv[1:] or ["-q"]))
