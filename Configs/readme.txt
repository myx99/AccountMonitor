<----- Please make below changes when deploy from windows to linux ----->
1. GlobalConfig.py
config_path = "/home/PerformanceAnalysis/Performance/AccountMonitor/Configs/

2.BPI_select.py
path = gc.getConfig('SubConfigPath', 'BPI_conf_linux')

3.BPI_html.py
path = config.getConfig('SubConfigPath', 'BPI_conf_linux')

4.API_select.py
path = config.getConfig('SubConfigPath', 'API_conf_linux')

5.API_html.py
path = config.getConfig('SubConfigPath', 'API_conf_linux')

6.composeMail.py
filename = '/home/PerformanceAnalysis/Performance/AccountMonitor/html/DailyBPI_report_%s.html' % ltd

7.bash script need to reformat to unix


8. Oracle table
wind.asharecalendar
wind.AShareEODPrices
wind.CBondEODPrices
wind.cbondanalysiscnbd