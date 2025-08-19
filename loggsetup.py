import logging
import argparse
import os

class LoggerSetup:
    def __init__(self):
        self.args = self._parse_args()
        self.logger = self._configure_logger()

    def _parse_args(self):
        try:
            parser = argparse.ArgumentParser(description='Script for file processing and log_utils')
            parser.add_argument('--debug', action='store_true', help='Enable debug log_utils')
            parser.add_argument('--log-file', default='logfile.log', help='Specify the log file path')
            args, _ = parser.parse_known_args()
            return args
        except SystemExit:
            return argparse.Namespace(debug=False, log_file='logfile.log')

    def _configure_logger(self):
        log_level = logging.DEBUG if self.args.debug else logging.INFO
        log_file = self.args.log_file

        logging.basicConfig(level=log_level, filemode="a", format='%(asctime)s - %(levelname)s - %(message)s')
        if not os.path.exists(log_file):
            open(log_file, 'w').close()

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger('').addHandler(file_handler)

        return logging.getLogger()
