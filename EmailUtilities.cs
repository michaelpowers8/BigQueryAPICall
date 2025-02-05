using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;
using NLog;

namespace BigQueryAPICall
{
	public static class EmailUtilities
	{
		public static Logger _logger = LogManager.GetCurrentClassLogger();
		public static void SendEmail(string to, string subject, string body)
		{
			try
			{
				MailMessage message = new MailMessage
				{
					Subject = subject,
					Body = body
				};
				message.To.Add(to);
				using (SmtpClient sc = new SmtpClient())
				{
					sc.Send(message);
				}
			}
			catch (Exception exc)
			{
				_logger.Error(exc, "Failed to send email to " + to + " \nSubject: " + subject + " \nBody: \n\n" + body);
			}
		}
	}
}
