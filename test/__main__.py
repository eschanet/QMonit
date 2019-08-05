import unittest

import argparse

import logging
from ..commonHelpers.logger import logger

logger = logger.getChild(__name__)

testmodules = [
    # 'commonHelpers',
    # 'maps',
    'scrapers',
    # 'writers',
    ]

parser = argparse.ArgumentParser(description='Run some tests')
parser.add_argument('tests', nargs='*', help='only run given tests')
parser.add_argument('--log', help='logging level')
parser.add_argument('--list', help='list test modules', action='store_true')
args = parser.parse_args()

if args.log:
    logger.parent.setLevel(getattr(logging, args.log.upper()))

if args.tests:
    tests = args.tests
else:
    tests = testmodules

if args.list:
    print("Possible test modules:")
    print("======================")
    for test in testmodules:
        print(test)
    sys.exit(0)

suite = unittest.TestSuite()

# based on code snippet from http://stackoverflow.com/questions/1732438/how-do-i-run-all-python-unit-tests-in-a-directory#15630454
for postfix in tests:
    t = "test."+postfix
    if "." in postfix:
        # i don't have a better solution yet, so hack for now
        importTest = ".".join(t.split(".")[:-2])
    else:
        importTest = t
    try:
        logger.info("Trying to import {}".format(importTest))
        mod = __import__(importTest, globals(), locals(), ['suite'])
    except ImportError:
        logger.error("Test {} not found - try {}".format(t, testmodules))
        raise
    try:
        # If the module defines a suite() function, call it to get the suite.
        suitefn = getattr(mod, 'suite')
        suite.addTest(suitefn())
    except (ImportError, AttributeError):
        # else, just load all the test cases from the module.
        logger.info("Loading test {}".format(t))
        suite.addTest(unittest.defaultTestLoader.loadTestsFromName(t))

result = unittest.TextTestRunner(verbosity=verbosity).run(suite)
sys.exit(not result.wasSuccessful())
