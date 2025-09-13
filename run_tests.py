#!/usr/bin/env python3
"""
Test runner for Simplified Reference Data Management System
Runs all unit tests and integration tests for the Excel workflow components
"""

import os
import sys
import unittest
import argparse
from pathlib import Path

# Add backend directory to path for imports
backend_dir = Path(__file__).parent / 'backend'
sys.path.insert(0, str(backend_dir))

def discover_and_run_tests(test_pattern='test_*.py', verbosity=2, failfast=False):
    """
    Discover and run tests in the backend/tests directory

    Args:
        test_pattern: Pattern for test files (default: 'test_*.py')
        verbosity: Test output verbosity (0=quiet, 1=normal, 2=verbose)
        failfast: Stop on first failure
    """
    # Setup test discovery
    test_dir = backend_dir / 'tests'

    if not test_dir.exists():
        print(f"Error: Test directory not found: {test_dir}")
        return False

    # Create test loader
    loader = unittest.TestLoader()

    # Discover tests
    try:
        test_suite = loader.discover(
            start_dir=str(test_dir),
            pattern=test_pattern,
            top_level_dir=str(backend_dir)
        )
    except Exception as e:
        print(f"Error discovering tests: {e}")
        return False

    # Run tests
    runner = unittest.TextTestRunner(
        verbosity=verbosity,
        failfast=failfast,
        buffer=True  # Capture stdout/stderr during test execution
    )

    print(f"Running tests from: {test_dir}")
    print(f"Test pattern: {test_pattern}")
    print("=" * 70)

    result = runner.run(test_suite)

    # Print summary
    print("=" * 70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")

    if result.failures:
        print("\nFAILURES:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback.split('AssertionError:')[-1].strip()}")

    if result.errors:
        print("\nERRORS:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback.split('Error:')[-1].strip()}")

    if result.skipped:
        print(f"\nSKIPPED ({len(result.skipped)} tests):")
        for test, reason in result.skipped:
            print(f"- {test}: {reason}")

    # Return success if no failures or errors
    return len(result.failures) == 0 and len(result.errors) == 0

def run_specific_test(test_name, verbosity=2):
    """
    Run a specific test module or test method

    Args:
        test_name: Name of test module or test method (e.g., 'test_excel_generator' or 'test_excel_generator.TestExcelGenerator.test_init')
        verbosity: Test output verbosity
    """
    try:
        # Import the test module
        test_module = __import__(f'tests.{test_name}', fromlist=[test_name])

        # Create test suite
        loader = unittest.TestLoader()
        suite = loader.loadTestsFromModule(test_module)

        # Run tests
        runner = unittest.TextTestRunner(verbosity=verbosity, buffer=True)
        result = runner.run(suite)

        return len(result.failures) == 0 and len(result.errors) == 0

    except ImportError as e:
        print(f"Error importing test module '{test_name}': {e}")
        return False
    except Exception as e:
        print(f"Error running test '{test_name}': {e}")
        return False

def check_dependencies():
    """Check if required dependencies for tests are available"""
    missing_deps = []

    try:
        import reportlab
    except ImportError:
        missing_deps.append('reportlab')

    try:
        import openpyxl
    except ImportError:
        missing_deps.append('openpyxl')

    if missing_deps:
        print("Warning: Missing dependencies for full test coverage:")
        for dep in missing_deps:
            print(f"  - {dep}")
        print("\nTo install missing dependencies:")
        print(f"  pip install {' '.join(missing_deps)}")
        print("\nSome tests will be skipped without these dependencies.")
        print()

    return len(missing_deps) == 0

def main():
    """Main test runner"""
    parser = argparse.ArgumentParser(description='Test runner for Simplified Dropoff System')
    parser.add_argument(
        '--test', '-t',
        help='Run specific test module (e.g., test_excel_generator)'
    )
    parser.add_argument(
        '--pattern', '-p',
        default='test_*.py',
        help='Test file pattern (default: test_*.py)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='count',
        default=1,
        help='Increase test output verbosity (use -v, -vv, or -vvv)'
    )
    parser.add_argument(
        '--failfast', '-f',
        action='store_true',
        help='Stop on first failure'
    )
    parser.add_argument(
        '--check-deps',
        action='store_true',
        help='Check test dependencies and exit'
    )

    args = parser.parse_args()

    # Check dependencies if requested
    if args.check_deps:
        all_deps_available = check_dependencies()
        sys.exit(0 if all_deps_available else 1)

    # Show dependency status
    check_dependencies()

    # Run specific test or discover all tests
    if args.test:
        print(f"Running specific test: {args.test}")
        success = run_specific_test(args.test, args.verbose)
    else:
        success = discover_and_run_tests(
            test_pattern=args.pattern,
            verbosity=args.verbose,
            failfast=args.failfast
        )

    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()