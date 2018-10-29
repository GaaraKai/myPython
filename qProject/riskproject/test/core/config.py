import datetime


class DeviceConfig:
    LOG_PATH = "D:/github_program/myPython/logs/logs.log"
    CSV_PATH = "D:/github_program/myPython/docs/csvfiles"
    RESULT_PATH = "D:/github_program/myPython/docs/rst/"
    PND_TD_DEVICE_FILE = "D:/github_program/myPython/docs/csvfiles/201801/pnd_td_device.csv"
    START_DATE = "2018-01-01"
    END_DATE = "2018-02-28"
    DATE_RANGE = (datetime.datetime.strptime(END_DATE, '%Y-%m-%d')
                  - datetime.datetime.strptime(START_DATE, '%Y-%m-%d')).days
    ID_HURDLE = 30000
    TRX_HURDLE = 60000


class MerIndustryConfig:
    mer_idst_dict = {"1": "电商商户"
        , "2": "平台"
        , "3": "基金商户"
        , "4": "彩票商户"
        , "5": "航旅商户"
        , "6": "P2P商户"
        , "7": "报销商户"
        , "8": "数字娱乐商户"
        , "9": "电渠"
        , "10": "行业资金归集"
        , "11": "个人金融业务线"
        , "12": "其它"
        , "13": "移动电商"
        , "14": "互联网理财"
        , "15": "消费金融"
        , "16": "代理商"
        , "17": "客服"
        , "18": "跨境"
        , "19": "聚合服务商"
        , "20": "电商三类"
        , "21": "银行(自用)"}
