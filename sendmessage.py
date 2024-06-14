import smtplib
from email.mime.text import MIMEText
import traceback

class Mail:
	
	def __init__(self):
		self.mailto_list=['xxx']	#收件人(列表)
		self.mail_host="smtp.163.com"	#使用的邮箱的smtp服务器地址，这里是163的smtp地址
		self.mail_user="xxx"	#用户名（网易邮箱登录的用户名）
		self.mail_pass="xxx"	#密码（注意：此处密码为网易邮箱授权码，用于第三方客户端登陆,登陆密码为yjs090310）

	def Send_Mail(self,sub,content):
		
		mailto_list = self.mailto_list
		mail_host = self.mail_host
		mail_user = self.mail_user
		mail_pass = self.mail_pass
		
		sender="<"+mail_user+">"
		msg = MIMEText(content,'html','utf-8')
		msg['Subject'] = sub
		msg['From'] = sender
		msg['To'] = ";".join(mailto_list)#将收件人列表以‘；’分隔
		try:
			server = smtplib.SMTP()		#创建smtp对象
			server.connect(mail_host)	#连接主机
			server.login(mail_user,mail_pass)	#登陆账号
			server.sendmail(sender, mailto_list, msg.as_string())	#发送邮件
			server.close()
			return True
		except:
			return traceback.format_exc()		#返回异常消息


def test():
	m = Mail()
	m.Send_Mail("xxx","test！")
	
