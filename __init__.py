# should use at least python 2.7
import sys
assert sys.version_info == (2,7), "QMonit needs python 2.7 - if you are in an slc environment consider using 'lsetup python'"

# user shouldn't use ROOT5
from .commonHelpers.logger import logger
logger = logger.getChild(__name__)

# import some classes into the global namespace
from .maps import Map
from .scrapers import Scraper
# from writers import Writer
