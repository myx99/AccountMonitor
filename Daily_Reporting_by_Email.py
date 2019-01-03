from Reporting.composeMail import composeHTML
from Common.Email import Email


html_msg = composeHTML()
x = Email()
x.sendEmail(html_msg)
