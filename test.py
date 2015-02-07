import ConfigParser

settings = ConfigParser.ConfigParser()

settings.read('config.ini')

print settings.get('sql_info', 'user')
