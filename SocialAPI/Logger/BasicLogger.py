import logging
import logging.config

class Logger(object):
	def __init__(self,configFile,loggerName):
		self.__configFile = configFile
		self.__loggerName = loggerName
		
	def createLogger(self):
		logging.config.fileConfig(self.__configFile)
		self.logger = logging.getLogger(self.__loggerName)
		return self.logger

	def __str__(self):
		return "Logger: " + self.__loggerName + " is chosen" 