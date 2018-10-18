# -*- coding=utf-8 -*-

from Common.Email import Email
from SQLStatement.BPI_html import BPIH
from Common.MySQLConnector import MySQLConnector
import pandas as pd

from email.mime.multipart import MIMEMultipart  # 构建邮件头信息，包括发件人，接收人，标题等
from email.mime.text import MIMEText  # 构建邮件正文，可以是text，也可以是HTML
from email.mime.application import MIMEApplication  # 构建邮件附件，理论上，只要是文件即可，一般是图片，Excel表格，word文件等
from email.header import Header  # 专门构建邮件标题的，这样做，可以支持标题中文



def composeHTML():
    #get dataframe
    m = BPIH()
    sqls = m.setBPIH()
    ms = MySQLConnector()
    connection = ms.getConn()
    df = pd.read_sql(sql=sqls, con=connection)
    # print(df)

    # convert to HTML
    df_html = df.to_html(escape=False)

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

    # 构造模板的附件（100）
    body = \
            """
            <body>

            <div align="center" class="header">
                <h1 align="center">资产管理事业部产品绩效分析日报 </h1>
            </div>

            <hr>

            <div class="content">
                <h2>华菁资管</h2>
                <div>
                    <h4></h4>
                    {df_html}
                </div>
            <hr>

                <p style="text-align: center">
                    —— 说明：目前测试阶段只有产品基础信息，常用指标信息赶制中 ——
                </p>
            </div>
            </body>
            """.format(df_html=df_html)
    html_msg = "<html>" + head + body + "</html>"
    # 这里是将HTML文件输出，作为测试的时候，查看格式用的，正式脚本中可以注释掉
    fout = open('D:\\PerformanceAnalysis\\html\\DailyBPI_report.html', 'w', encoding='UTF-8', newline='')
    fout.write(html_msg)
    return html_msg


html_msg = composeHTML()
x = Email()
x.sendEmail(html_msg)






