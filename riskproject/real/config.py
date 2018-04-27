import datetime
import pandas as pd


class DeviceConfig:
    LOG_PATH = "D:/github_program/myPython/logs/logs.log"
    CSV_PATH = "D:/github_program/myPython/docs/csvfiles"
    CONF_PATH = "D:/github_program/myPython/docs/conf/"
    RESULT_PATH = "D:/github_program/myPython/docs/rst/"
    START_DATE = "2018-04-01"
    END_DATE = "2018-04-20"
    DATE_RANGE = (datetime.datetime.strptime(END_DATE, '%Y-%m-%d')
                  - datetime.datetime.strptime(START_DATE, '%Y-%m-%d')).days + 1
    START_DATE = pd.Timestamp(2018, 4, 1)
    END_DATE = pd.Timestamp(2018, 4, 20)
    DATE_RANGE = END_DATE - START_DATE
    ID_HURDLE = 30
    TRX_HURDLE = 60000


class SimilarityConfig:
    LOG_PATH = "D:/github_program/myPython/logs/logs.log"
    CSV_PATH = "D:/github_program/myPython/docs/csvfiles"
    CONF_PATH = "D:/github_program/myPython/docs/conf/"
    RESULT_PATH = "D:/github_program/myPython/docs/rst/"
    START_DATE = pd.Timestamp(2018, 4, 1)
    END_DATE = pd.Timestamp(2018, 4, 20)
    DATE_RANGE = END_DATE - START_DATE
