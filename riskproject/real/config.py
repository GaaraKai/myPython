import datetime


class DeviceConfig:
    LOG_PATH = "D:/github_program/myPython/logs/logs.log"
    CSV_PATH = "D:/github_program/myPython/docs/csvfiles"
    RESULT_PATH = "D:/github_program/myPython/docs/rst/"
    PND_TD_DEVICE_FILE = "D:/github_program/myPython/docs/csvfiles/device_bl_analysis/pnd_td_device.csv"
    START_DATE = "2018-01-01"
    END_DATE = "2018-01-05"
    DATE_RANGE = (datetime.datetime.strptime(END_DATE, '%Y-%m-%d')
                  - datetime.datetime.strptime(START_DATE, '%Y-%m-%d')).days + 1
    ID_HURDLE = 30
    TRX_HURDLE = 60000
