import logging
import sys
from awsglue.utils import getResolvedOptions

logger = logging.getLogger('run_query')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

def run_query():
  options = get_options()

  set_logger_level(options)

  logger.info(options)

def get_options():
    optionNames = [
      'debugLogging',
      'calendarYear',
      'surveyType',
      'stage',
      's3Bucket',
      's3Key',
      'tenantId',
      'userId',
    ]

    options = getResolvedOptions(sys.argv, optionNames)

    return options

def set_logger_level(options):
  if (options['debugLogging'].lower() == 'true'):
    logger.setLevel(logging.DEBUG)
    logger.debug('Debug logging enabled.')

if __name__ == '__main__':
  run_query()
