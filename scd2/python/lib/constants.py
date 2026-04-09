from datetime import datetime

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
MAX_TS_STR = "9999-12-31 23:59:59"
MAX_TS = datetime.strptime(MAX_TS_STR, DATE_FORMAT)
