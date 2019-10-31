# -*- coding=utf-8 -*-

from Common.Email import Email
from SQLStatement.BPI_html import BPIH
from SQLStatement.API_html import APIH
from SQLStatement.AnnualizedReturn_Monitor_html import ANRM
from Common.TradingDay import TradingDay
from Common.MySQLConnector import MySQLConnector
import pandas as pd


from email.mime.multipart import MIMEMultipart  # 构建邮件头信息，包括发件人，接收人，标题等
from email.mime.text import MIMEText  # 构建邮件正文，可以是text，也可以是HTML
from email.mime.application import MIMEApplication  # 构建邮件附件，理论上，只要是文件即可，一般是图片，Excel表格，word文件等
from email.header import Header  # 专门构建邮件标题的，这样做，可以支持标题中文



def composeHTML():
    #get dataframe
    bpih = BPIH()
    apih = APIH()
    anrm = ANRM()
    sqls_b = bpih.setBPIH()
    sqls_a = apih.setAPIH()
    sqls_m = anrm.setANRM()

    ms = MySQLConnector()
    connection = ms.getConn()
    df_b = pd.read_sql(sql=sqls_b, con=connection)
    df_a = pd.read_sql(sql=sqls_a, con=connection)
    df_m = pd.read_sql(sql=sqls_m, con=connection)
    # print(df)

    # convert to HTML
    df_html_b = df_b.to_html(escape=False)
    df_html_a = df_a.to_html(escape=False)
    df_html_m = df_m.to_html(escape=False)

    # compose HTML
    head = \
            """
            <head>
                <meta charset="utf-8">
                <STYLE TYPE="text/css" MEDIA=screen>

                    table.dataframe {
                        border-collapse: collapse;
                        border: 2px solid #a19da2;
                        /*居中显示整个表格*/
                        margin: auto;
                    }

                    table.dataframe thead {
                        border: 2px solid #91c6e1;
                        background: #f1f1f1;
                        padding: 10px 10px 10px 10px;
                        color: #333333;
                    }

                    table.dataframe tbody {
                        border: 2px solid #91c6e1;
                        padding: 10px 10px 10px 10px;
                    }

                    table.dataframe tr {

                    }

                    table.dataframe th {
                        vertical-align: top;
                        font-size: 14px;
                        padding: 10px 10px 10px 10px;
                        color: #105de3;
                        font-family: arial;
                        text-align: center;
                    }

                    table.dataframe td {
                        text-align: center;
                        padding: 10px 10px 10px 10px;
                    }

                    body {
                        font-family: 宋体;
                    }

                    h1 {
                        color: #5db446
                    }

                    div.header h2 {
                        color: #0002e3;
                        font-family: 黑体;
                    }

                    div.content h2 {
                        text-align: center;
                        font-size: 28px;
                        text-shadow: 2px 2px 1px #de4040;
                        color: #fff;
                        font-weight: bold;
                        background-color: #008eb7;
                        line-height: 1.5;
                        margin: 20px 0;
                        box-shadow: 10px 10px 5px #888888;
                        border-radius: 5px;
                    }

                    h3 {
                        font-size: 22px;
                        background-color: rgba(0, 2, 227, 0.71);
                        text-shadow: 2px 2px 1px #de4040;
                        color: rgba(239, 241, 234, 0.99);
                        line-height: 1.5;
                    }

                    h4 {
                        color: #e10092;
                        font-family: 楷体;
                        font-size: 20px;
                        text-align: center;
                    }

                    td img {
                        /*width: 60px;*/
                        max-width: 300px;
                        max-height: 300px;
                    }

                </STYLE>
            </head>
            """

    # 构造模板的附件
    body = \
            """
            <body>

            <div align="center" class="header">
                <h1 align="center">资产管理事业部产品绩效分析日报 </h1>
            </div>

            <hr>

            <div class="content">
                <h2>报价产品年化收益率监控</h2>
                <div>
                    <h4></h4>
                    {df_html_m}
                <div>
                <h2>估值信息</h2>
                <div>
                    <h4></h4>
                    {df_html_b}
                <div>
                <h2>常用指标信息</h2>
                <div>
                    <h4></h4>
                    {df_html_a}
                </div>
            <hr>

                <p style="text-align: center">
                    —— 说明：估值信息从每日估值表筛选获取，常用指标信息根据估值信息计算得出 ——
                </p>
            </div>
            </body>
            """.format(df_html_m=df_html_m,df_html_b=df_html_b,df_html_a=df_html_a)
    html_msg = "<html>" + head + body + "</html>"
    # 这里是将HTML文件输出，作为测试的时候，查看格式用的，正式脚本中可以注释掉
    td = TradingDay()
    ltd = td.getLastTradingDay()
    filename = 'D:\\PerformanceAnalysis\\html\\DailyBPI_report_%s.html' % ltd
    # filename = '/home/PerformanceAnalysis/Performance/AccountMonitor/html/DailyBPI_report_%s.html' % ltd

    fout = open(filename, 'w', encoding='UTF-8', newline='')
    fout.write(html_msg)
    return html_msg


# html_msg = composeHTML()
# x = Email()
# x.sendEmail(html_msg)






