# -*- coding=utf-8 -*-

from Common.GlobalConfig import GlobalConfig
import time
import smtplib
from email.mime.text import MIMEText
import os
from email.mime.multipart import MIMEMultipart
from email.mime.multipart import MIMEBase
import mimetypes
from email.mime.image import MIMEImage
from email.encoders import encode_base64
from email.header import Header
from Common.TradingDay import TradingDay



class Email(object):
    def __init__(self):
        config = GlobalConfig()
        self.mailserver = config.getConfig('email', 'mailserver')
        self.from_addr = config.getConfig('email', 'from_addr')
        self.sender_password = config.getConfig('email', 'sender_password')
        self.recipient = config.getConfig('email', 'recipient')
        td = TradingDay()
        self.date_of_subject = td.getLastTradingDay()
        self.subject = config.getConfig('email', 'subject') + self.date_of_subject
        self.attachmentFilePath = config.getConfig('email', 'attachmentFilePath')

    def sendEmail(self, html_msg):
        svr = smtplib.SMTP(self.mailserver, 465)
        svr.starttls()
        svr.docmd("HELO server")
        svr.login(self.from_addr, self.sender_password)
        msg = MIMEMultipart()

        msg['subject'] = Header(self.subject)
        msg['from'] = self.from_addr
        msg['to'] = self.recipient

        # print(self.recipient)

        content_html = MIMEText(html_msg, "html", "utf-8")
        msg.attach(content_html)

        # if self.attachmentFilePath != 'none':
        #     msg.attach(self.getAttachment())
        svr.sendmail(self.from_addr, self.recipient, msg.as_string())
        svr.quit()

    # def getAttachment(self):
    #     contentType, encoding = mimetypes.guess_type(self.attachmentFilePath)
    #     if contentType is None or encoding is not None:
    #         contentType = 'application/octet-stream'
    #     mainType, subType = contentType.split('/', 1)
    #     file = open(self.attachmentFilePath, 'rb')
    #     if mainType == 'text':
    #         attachment = MIMEBase(mainType, subType)
    #         attachment.set_payload(file.read())
    #         encode_base64(attachment)
    #     elif mainType == 'image':
    #         attachment = MIMEImage(file.read())
    #     else:
    #         attachment = MIMEBase(mainType, subType)
    #         attachment.set_payload(file.read())
    #         encode_base64(attachment)
    #     file.close()
    #     attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(self.attachmentFilePath))
    #     return attachment


if __name__ == '__main__':
    x = Email()
    # content = open("D:\\PerformanceAnalysis\\EmailFormat.html", 'rb').read()
    content = MIMEText("test", "text", "utf-8")
    x.sendEmail(content)

