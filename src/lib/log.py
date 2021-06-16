"""A class for logging."""
from __future__ import print_function
import datetime

class Log(object):
    """A class for logging output for a Glue job.

    Args:
        object (Object): Base class.
    """

    def __init__(self, glue_logger):
        """Constructor.

        Args:
            glue_logger (GlueLogger): The interface used to log AWS Glue job
                messages.
        """
        self.glue_logger = glue_logger
        pass

    def _log(self,log_type, job_id, message,glue_logging):
        """Logs a message.

        Args:
            log_type (str): Logging level ('info', 'warn', 'error')
            job_id (str): AWS Glue job id.
            message (str): The message to log.
            glue_logging (function): the GlueLogger function to invoke.
        """
        info_log = {'@job_id': job_id,
                    '@date_time': str(datetime.datetime.now()),
                    '@log_type': log_type,
                    '@message': message }

        print(str(info_log))
        glue_logging(str(info_log))
#        return str(info_log)

    def info(self, job_id, message):
        """Logs a message at the 'info' logging level.

        Args:
            job_id (str): AWS Glue job id.
            message (str): The message to log.

        Returns:
            None: no value is returned
        """
        return self._log(log_type='INFO', job_id=job_id, message=message, glue_logging = self.glue_logger.info) # todo: do not return a value

    def warn(self, job_id, message):
        """Logs a message at the 'warn' logging level.

        Args:
            job_id (str): AWS Glue job id.
            message (str): The message to log.

        Returns:
            None: no value is returned
        """
        return self._log(log_type='INFO', job_id=job_id, message=message, glue_logging = self.glue_logger.warn) # todo: do not return a value

    def error(self, job_id, message):
        """Logs a message at the 'error' logging level.

        Args:
            job_id (str): AWS Glue job id.
            message (str): The message to log.

        Returns:
            None: no value is returned
        """
        return self._log(log_type='ERROR', job_id=job_id, message=message, glue_logging = self.glue_logger.error) # todo: do not return a value