from ib_insync.ibcontroller import IBC
from credentials import creds

ibc = IBC(twsVersion=972,
          gateway=True,
          tradingMode='paper',
          userid=creds['userid'],
          password=creds['password'],
          )
