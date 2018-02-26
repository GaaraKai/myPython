import datetime


class DeviceConfig:
    LOG_PATH = "D:/github_program/myPython/logs/logs.log"
    RESULT_PATH = "D:/github_program/myPython/docs/rst/"
    START_DATE = "2018-01-01"
    END_DATE = "2018-01-10"
    DATE_RANGE = (datetime.datetime.strptime(END_DATE, '%Y-%m-%d')
                  - datetime.datetime.strptime(START_DATE, '%Y-%m-%d')).days
    ID_HURDLE = 30
    TRX_HURDLE = 60000
