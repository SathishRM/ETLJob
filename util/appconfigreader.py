from configparser import ConfigParser
import os


class AppConfigReader():
    '''Loads the config file values'''

    def __init__(self, cfgFile):
        self.config = ConfigParser()
        # Get the config file path from environmental variable PY_APP_CONFIG
        #cfgDir = os.environ.get('CFG_DIR')
        if not cfgFile:
        #     cfgFile = cfgFile
        # else:
            cfgFile = "E:\\Spark\\github\\ETLJob\\conf\\mergetransform.properties"

        # Load the CFG file
        if os.path.isfile(cfgFile):
            self.config.read(cfgFile)

