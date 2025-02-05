using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration; // Accessing the App.config file
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml;
using Newtonsoft.Json;
using NLog;
using Google.Cloud.BigQuery.V2;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Logging;
using Microsoft.Extensions.Configuration;
using CsvHelper;
using System.Globalization;

namespace BigQueryAPICall
{
	class Program
	{
		public static int startDateOffset;
		public static int endDateOffset;
		public static DateTime startDateOverride;
		public static DateTime endDateOverride;
		public static List<DateTime> allDatesToExtractData;
		public static bool error_occurred = false;
		public static string emailError = "";
		public static readonly NameValueCollection appSettings = System.Configuration.ConfigurationManager.AppSettings;
		public static void Main(string[] args)
		{
			Logger _logger = LogManager.GetCurrentClassLogger();
			if (args.Length == 0)
			{
				Console.WriteLine("Pass ? for a list of parameters");
				return;
			}

			if (args[0] == "?")
			{
				Console.WriteLine("Available parameters:");
				Console.WriteLine("[-all] - Request all available tables");
				Console.WriteLine("[-alldate] - Request all Dated tables");
				Console.WriteLine("[-allnodate] - Request all Non-Dated tables");
				Console.WriteLine("[-itemdate] - Request all ITEM tables with Dates");
				Console.WriteLine("[-itemnodate] - Request all ITEM tables without Dates");
				Console.WriteLine("[-allitem] - Request all ITEM tables");
				Console.WriteLine("[-impdate] - Request all IMP tables with Dates");
				Console.WriteLine("[-impnodate] - Request all IMP tables without Dates");
				Console.WriteLine("[-allimp] - Request all IMP tables");
				Console.WriteLine("[-pidate] - Request all PI tables with Dates");
				Console.WriteLine("[-pinodate] - Request all PI tables without Dates");
				Console.WriteLine("[-allpi] - Request all PI tables");
				Console.WriteLine("[-test] - Request all available tables and send them to the testing file specified in App.config");
				return;
			}

			bool itemmvmtDates = false; 
			bool itemmvmtNoDates = false;
			bool impDates = false;
			bool impNoDates = false;
			bool piDates = false;
			bool piNoDates = false;
			bool testing = false;
			string nonDatedTablesToExtractToString = "";
			string toEmail = System.Configuration.ConfigurationManager.AppSettings["FailureMessageToEmail"];

			foreach (var arg in args) // based on which parameter is called in args decides which set of tables will be extracted
			{
				if (arg == "-all")
				{
					itemmvmtDates = true;
					itemmvmtNoDates = true;
					impDates = true;
					impNoDates = true;
					piDates = true;
					piNoDates = true;
					break;
				}

				else if (arg == "-test")
				{
					itemmvmtDates = true;
					itemmvmtNoDates = true;
					impDates = true;
					impNoDates = true;
					piDates = true;
					piNoDates = true;
					testing = true;
					break;
				}

				switch (arg?.ToLower())
				{
					case "-itemdate":
						itemmvmtDates = true;
						break;

					case "-itemnodate":
						itemmvmtNoDates = true;
						break;

					case "-allitem":
						itemmvmtDates = true;
						itemmvmtNoDates = true;
						break;
					case "-impdate":
						impDates = true;
						break;

					case "-impnodate":
						impNoDates = true;
						break;

					case "-allimp":
						impDates = true;
						impNoDates = true;
						break;
					case "-pidate":
						piDates = true;
						break;

					case "-pinodate":
						piNoDates = true;
						break;

					case "-allpi":
						piDates = true;
						piNoDates = true;
						break;

					case "-allnodate":
						itemmvmtNoDates = true;
						impNoDates = true;
						piNoDates = true;
						break;

					case "-alldate":
						itemmvmtDates = true;
						impDates = true;
						piDates = true;
						break;

					/* If parameter passed is not any of the above cases, log an error that an invalid parameter was passed, 
					 * display the permitted parameters to the console, and terminate the program. */
					default: 
						_logger.Error("Invalid Parameter passed");
						Console.WriteLine("Invalid Parameter passed");
						Console.WriteLine("Available parameters:");
						Console.WriteLine("[-all] - Request all available tables");
						Console.WriteLine("[-alldate] - Request all Dated tables");
						Console.WriteLine("[-allnodate] - Request all Non-Dated tables");
						Console.WriteLine("[-itemdate] - Request all ITEM tables with Dates");
						Console.WriteLine("[-itemnodate] - Request all ITEM tables without Dates");
						Console.WriteLine("[-allitem] - Request all ITEM tables");
						Console.WriteLine("[-impdate] - Request all IMP tables with Dates");
						Console.WriteLine("[-impnodate] - Request all IMP tables without Dates");
						Console.WriteLine("[-allimp] - Request all IMP tables");
						Console.WriteLine("[-pidate] - Request all PI tables with Dates");
						Console.WriteLine("[-pinodate] - Request all PI tables without Dates");
						Console.WriteLine("[-allpi] - Request all PI tables");
						Console.WriteLine("[-test] - Request all available tables and send them to the testing file specified in App.config");
						return;
				}
			}

			#region Getting Tables 
			// Creating the list of tables that data will be extracted from based on the booleans set to true previously.
			_logger.Info("Retrieving the names of all tables data will be extracted.");
			Console.WriteLine("Retrieving the names of all tables data will be extracted.");
			List<string> datedTablesToExtract = new List<string>();
			List<string> nonDatedTablesToExtract = new List<string>();
			if (itemmvmtDates)
			{
				datedTablesToExtract.AddRange(GetAllDatedITEMTables(appSettings));
			}

			if (itemmvmtNoDates)
			{
				nonDatedTablesToExtract.AddRange(GetAllNonDatedITEMTables(appSettings));
			}

			if (impDates)
			{
				datedTablesToExtract.AddRange(GetAllDatedIMPTables(appSettings));
			}

			if (impNoDates)
			{
				nonDatedTablesToExtract.AddRange(GetAllNonDatedIMPTables(appSettings));
			}

			if (piDates)
			{
				datedTablesToExtract.AddRange(GetAllDatedPITables(appSettings));
			}

			if (piNoDates)
			{
				nonDatedTablesToExtract.AddRange(GetAllNonDatedPITables(appSettings));
			}
			#endregion

			#region Extraction Dates
			_logger.Info("Setting all dates that will be used in dated extractions");
			Console.WriteLine("Setting all dates that will be used in dated extractions");
			/* Setting the Overriding start and end dates values and if possible, create a list of all dates to extract between 
			 * offsetting dates. If the override dates are null or not in a format that DateTime.Parse works, override dates
			 * are set to 1900/01/01 and the extraction dates list is set to null. 
			 * THIS IS NOT A PROBLEM BECAUSE OVERRIDES ARE NOTALWAYS MADE*/
			startDateOverride = GetDateOverride("DatedDataCollectionsStartDateOverride", _logger);
			endDateOverride = GetDateOverride("DatedDataCollectionsEndDateOverride", _logger);
			if (
				(startDateOverride.Year == 1900) ||
				(endDateOverride.Year == 1900) ||
				(startDateOverride.Year != 2024 && startDateOverride.Year != 2025)
			)
			{
				allDatesToExtractData = null;
			}
			else
			{
				allDatesToExtractData = GetAllDatesFromOverrideValues(startDateOverride, endDateOverride, _logger);
				_logger.Info($"Successfully created all extraction dates from {startDateOverride.Date}-{endDateOverride.Date}");
				Console.WriteLine($"Successfully created all extraction dates from {startDateOverride.Date}-{endDateOverride.Date}");
			}

			/* This will first check if override dates were used. If they were, this section is skipped over. If the extraction dates
			 * are null, then dates are created based on the offsetting values. These values must always be negative because it is 
			 * going to be extracting past data. Future data does not exist. If this fails, the default dates will be 30 days from 
			 * today and 1 day from today. This will signal a warning in the logger and on the Console if the */
			if (allDatesToExtractData == null)
			{
				startDateOffset = GetOffsetDates(appSettings, "DatedDataCollectionsStartDateOffset", _logger);
				endDateOffset = GetOffsetDates(appSettings, "DatedDataCollectionsEndDateOffset", _logger);
				if (startDateOffset < 0 && endDateOffset < 0 && startDateOffset < endDateOffset)
				{
					allDatesToExtractData = GetAllDatesFromOffsettingValues(startDateOffset, endDateOffset, _logger);
				}
				else if (startDateOffset < 0 && endDateOffset < 0 && startDateOffset > endDateOffset)
				{
					_logger.Error($"Offset dates were pulled, but the starting date was after the ending date. Start date must be less than 0 and less than end date.");
					Console.WriteLine($"Offset dates were pulled, but the starting date was after the ending date. Start date must be less than 0 and less than end date.");
					Program.error_occurred = true;
					Program.emailError += "Offset dates were pulled, but the starting date was after the ending date. Start date must be less than 0 and less than end date.\n";
				}
				else if (startDateOffset > 0 || endDateOffset > 0)
				{
					_logger.Error($"Offset dates were pulled, but either the starting date or ending date were greater than 0. Start date and end date must be less than 0");
					Console.WriteLine($"Offset dates were pulled, but either the starting date or ending date were greater than 0. Start date and end date must be less than 0");
					Program.error_occurred = true;
					Program.emailError += "Offset dates were pulled, but either the starting date or ending date were greater than 0. Start date and end date must be less than 0\n";
				}
				if (allDatesToExtractData == null)
				{
					_logger.Error("No valid date range found using offset dates or override dates. Any tables with dated columns selected will NOT be extracted.");
					Console.WriteLine("No valid date range found using offset dates or override dates. Any tables with dated columns selected will NOT be extracted.");
					Program.error_occurred = true;
					Program.emailError += "No valid date range found using offset dates or override dates. Any tables with dated columns selected will NOT be extracted.\n";
				}
				else
				{
					_logger.Info($"Successfully created all extraction dates from {allDatesToExtractData[0].Year}/{allDatesToExtractData[0].Month}/{allDatesToExtractData[0].Day}-{allDatesToExtractData[allDatesToExtractData.Count - 1].Year}/{allDatesToExtractData[allDatesToExtractData.Count - 1].Month}/{allDatesToExtractData[allDatesToExtractData.Count - 1].Day}");
					Console.WriteLine($"Successfully created all extraction dates from {allDatesToExtractData[0].Year}/{allDatesToExtractData[0].Month}/{allDatesToExtractData[0].Day}-{allDatesToExtractData[allDatesToExtractData.Count - 1].Year}/{allDatesToExtractData[allDatesToExtractData.Count - 1].Month}/{allDatesToExtractData[allDatesToExtractData.Count - 1].Day}");
				}
			}
			#endregion

			#region BigQuery Credentials
			/* Setting the BigQuery Credentials provided by Schnucks directly from file.
			 * A copy of these credentials were added in App.config for reference but are not directly used.
			 * THIS PROGRAM WILL IMMEDIATELY STOP IF schnucks-atg-sa-f87620578592.json IS NOT FOUND*/
			string credentialsPath = "schnucks-atg-sa-f87620578592.json";
			GoogleCredential credential;
			BigQueryClient client;
			try
			{
				credential = GoogleCredential.FromFile(credentialsPath);
				client = BigQueryClient.Create("schnucks-datalake-prod", credential); 
			}
			catch (Exception e)
			{
				_logger.Error($"BigQuery Credentials could not be verified.\nError: {e.ToString()}");
				Console.WriteLine($"BigQuery Credentials could not be verified.\nError: {e.ToString()}");
				EmailUtilities.SendEmail(toEmail, "BigQuery Credentials", $"BigQuery Credentials could not be verified.\nError: {e.ToString()}");
				return;
			}
			#endregion

			#region Data Extraction
			// All tables that do not have a dated column will be extracted entirely first due to being much smaller.
			if (nonDatedTablesToExtract.Count > 0)
			{
				CreateTablesWithoutDateRange(nonDatedTablesToExtract, client, appSettings, testing, _logger);
				
				foreach (string table in nonDatedTablesToExtract)
				{
					nonDatedTablesToExtractToString += $"{table}, ";
				}
				/* If all data has been successfully extracted, and saved with no errors of any kind, send a success email
				 * out to the email designated in App.config, log the success and display the success to the console. */
				Console.WriteLine($"Big Query API data for tables {nonDatedTablesToExtractToString} have been successfully extracted.");
				_logger.Info($"Big Query API data for tables {nonDatedTablesToExtractToString} have been successfully extracted.");
			}

			/* All tables with a dated column will now be extracted. Every table for a single day will be extracted
			 * before moving on to the next day.*/
			if (datedTablesToExtract.Count > 0 && allDatesToExtractData!=null)
			{
				CreateTablesWithDateRange(allDatesToExtractData, datedTablesToExtract, client, appSettings, testing, _logger);
				string datedTablesToExtractToString = "";
				foreach (string table in datedTablesToExtract)
				{
					datedTablesToExtractToString += $"{table}, ";
				}
				/* If all data has been successfully extracted, and saved with no errors of any kind, send a success email
				 * out to the email designated in App.config, log the success and display the success to the console. */
				Console.WriteLine($"Big Query API data for tables {datedTablesToExtractToString} have been successfully extracted.");
				_logger.Info($"Big Query API data for tables {datedTablesToExtractToString} have been successfully extracted.");
			}
			#endregion

			#region Emailing Out Problems
			if (Program.error_occurred)
			{ 
				EmailUtilities.SendEmail(toEmail, "Schnucks Big Query Problems", $"{Program.emailError}"); 
			}
			#endregion
		}

		#region Getting Extraction Dates
		public static int GetOffsetDates(NameValueCollection settings, string variableDate, Logger _logger)
		{
			try
			{
				int offset = Int32.Parse(settings[variableDate]); // Attempts to pull the DatedDataCollections****DateOffset value in App.config
				if (offset > 0) //Throw a warning message to the logger and the console if the collected value is greater than 0. Must be less than or equal to 0.
				{
					if (variableDate.ToLower().Contains("start"))
					{
						_logger.Error($"Retrieving offset dates for {variableDate} succeeded but had an invalid value of {offset}.\nError: {variableDate} is greater than 0 and must be less than or equal to 0.");
						Console.WriteLine($"Retrieving offset dates for {variableDate} succeeded but had an invalid value of {offset}.\nError: {variableDate} is greater than 0 and must be less than or equal to 0.");
						Program.error_occurred = true;
						Program.emailError += $"Retrieving offset dates for {variableDate} succeeded but had an invalid value of {offset}.\nError: {variableDate} is greater than 0 and must be less than or equal to 0.\n";
						return offset;
					}
					else
					{
						_logger.Error($"Retrieving offset dates for {variableDate} succeeded but had an invalid value of {offset}.\nError: {variableDate} is greater than 0 and must be less than or equal to 0.");
						Console.WriteLine($"Retrieving offset dates for {variableDate} succeeded but had an invalid value of {offset}.\nError: {variableDate} is greater than 0 and must be less than or equal to 0.");
						Program.error_occurred = true;
						Program.emailError += $"Retrieving offset dates for {variableDate} succeeded but had an invalid value of {offset}.\nError: {variableDate} is greater than 0 and must be less than or equal to 0.\n";
						return offset;
					}
				}
				else 
				{ 
					return offset; 
				}
				
			}
			catch // If DatedDataCollections****DateOffset fails to pull, throw an error message to the logger and send the identical message to the console.
			{
				if (variableDate.ToLower().Contains("start"))
				{
					_logger.Error($"Retrieving offset dates failed. Setting {variableDate} to default value -30.\nError: {variableDate} is greater than 0 and must be less than or equal to 0.");
					Console.WriteLine($"Retrieving offset dates failed. Setting {variableDate} to default value -30.\nError: {variableDate} is greater than 0 and must be less than or equal to 0.");
					Program.emailError += $"Retrieving offset dates failed. Setting {variableDate} to default value -30.\nError: {variableDate} is greater than 0 and must be less than or equal to 0.\n";
					Program.error_occurred = true;
					return -30;
				}
				else
				{
					_logger.Error($"Retrieving offset dates failed. Setting {variableDate} to default value -1.\nError: {variableDate} is greater than 0 and must be less than or equal to 0.");
					Console.WriteLine($"Retrieving offset dates failed. Setting {variableDate} to default value -1.\nError: {variableDate} is greater than 0 and must be less than or equal to 0.");
					Program.emailError += $"Retrieving offset dates failed. Setting {variableDate} to default value -1.\nError: {variableDate} is greater than 0 and must be less than or equal to 0.\n";
					Program.error_occurred = true;
					return -1;
				}
			}
		}

		public static DateTime GetDateOverride(string variableDate, Logger _logger)
		{
			try
			{
				return DateTime.Parse(System.Configuration.ConfigurationManager.AppSettings[variableDate]);
			}
			catch 
			{
				_logger.Info($"No override date found for {variableDate}.");
				Console.WriteLine($"No override date found for {variableDate}.");
				return new DateTime(1900, 1, 1);
			}
		}

		public static List<DateTime> GetAllDatesFromOffsettingValues(int startDate, int endDate, Logger _logger)
		{
			if (startDate >= endDate)
			{
				_logger.Error($"Starting date must be before the ending date. \nExample: \n\tstartDate=-30\n\tendDate=-1\nOffsetting Dates passed: {startDate}, {endDate}");
				Console.WriteLine($"Starting date must be before the ending date. \nExample: \n\tstartDate=-30\n\tendDate=-1\nOffsetting Dates passed: {startDate}, {endDate}");
				Program.emailError += $"Starting date must be before the ending date. \nExample: \n\tstartDate=-30\n\tendDate=-1\nOffsetting Dates passed: {startDate}, {endDate}\n";
				Program.error_occurred = true;
				return null;
			}
			DateTime start = DateTime.Today.AddDays(startDate);
			DateTime end = DateTime.Today.AddDays(endDate);
			List<DateTime> results = new List<DateTime>();
			for (DateTime dt = start; dt <= end; dt = dt.AddDays(1))
			{
				results.Add(dt);
			}
			return results;
		}

		public static List<DateTime> GetAllDatesFromOverrideValues(DateTime start, DateTime end, Logger _logger)
		{
			/* Create a list of all dates that are between the start and end date. This function will return null
			 * and issue a warning to the logger and to the console if two valid dates are found but the 
			 * start date is later than the end date. It will not assume you had it backwards. If either 
			 * the start date or end date are null, the logger will inform that no overriding dates were found
			 * and will return null. */
			if (start != null && end != null && start >= end)
			{
				_logger.Warn($"Starting date and ending date found, but start date must be before the ending date. \nExample: \n\tstartDate=2024/01/01\n\tendDate=2024/12/31\nActual Dates Passed: Start={start} , End={end}");
				Console.WriteLine($"Starting date and ending date found, but start date must be before the ending date. \nExample: \n\tstartDate=2024/01/01\n\tendDate=2024/12/31\nActual Dates Passed: Start={start} , End={end}");
				return null;
			}

			if (start == null || end == null)
			{
				_logger.Info("No Overriding Dates Found.");
				Console.WriteLine("No Overriding Dates Found.");
				return null;
			}

			List<DateTime> dates = new List<DateTime>();

			for (DateTime date = start; date <= end; date = date.AddDays(1))
			{
				dates.Add(date);
			}

			return dates;
		}
		#endregion

		#region Getting ITEM Tables
		public static List<string> GetAllITEMTables(NameValueCollection settings)
		{
			/* Read through App.config, and loop through all app settings. Any key that contains ITEM will
			 * be added to the list of tables to extract */
			List<string> tablesToExtract = new List<string>();
			foreach (string key in settings.AllKeys)
			{
				try
				{
					if (
						(
							key.Contains("ITEM")
						)
					)
					{
						tablesToExtract.Add(key);
					}
				}
				catch (Exception e)
				{
					Console.WriteLine(e.ToString());
				}
			}
			return tablesToExtract;
		}

		public static List<string> GetAllDatedITEMTables(NameValueCollection settings)
		{
			/* Read through App.config, and loop through all app settings. Any key that contains ITEM 
			 * and where its value does NOT contain "*" be added to the list of tables to extract */
			List<string> tablesToExtract = new List<string>();
			foreach (string key in settings.AllKeys)
			{
				try
				{
					if (
						(
							key.Contains("ITEM") &&
							(!settings[key].Contains("*"))
						)
					)
					{
						tablesToExtract.Add(key);
					}
				}
				catch (Exception e)
				{
					Console.WriteLine(e.ToString());
				}
			}
			return tablesToExtract;
		}

		public static List<string> GetAllNonDatedITEMTables(NameValueCollection settings)
		{
			/* Read through App.config, and loop through all app settings. Any key that contains ITEM 
			 * and where its value DOES contain "*" be added to the list of tables to extract */
			List<string> tablesToExtract = new List<string>();
			foreach (string key in settings.AllKeys)
			{
				try
				{
					if (
						(
							key.Contains("ITEM") &&
							(settings[key].Contains("*"))
						)
					)
					{
						tablesToExtract.Add(key);
					}
				}
				catch (Exception e)
				{
					Console.WriteLine(e.ToString());
				}
			}
			return tablesToExtract;
		}
		#endregion

		#region Getting IMP Tables
		public static List<string> GetAllIMPTables(NameValueCollection settings)
		{
			List<string> tablesToExtract = new List<string>();
			foreach (string key in settings.AllKeys)
			{
				try
				{
					if (
						(
							key.Contains("IMP")
						)
					)
					{
						tablesToExtract.Add(key);
					}
				}
				catch (Exception e)
				{
					Console.WriteLine(e.ToString());
				}
			}
			return tablesToExtract;
		}
		public static List<string> GetAllDatedIMPTables(NameValueCollection settings)
		{
			List<string> tablesToExtract = new List<string>();
			foreach (string key in settings.AllKeys)
			{
				try
				{
					if (
						(
							key.Contains("IMP") &&
							(!settings[key].Contains("*"))
						)
					)
					{
						tablesToExtract.Add(key);
					}
				}
				catch (Exception e)
				{
					Console.WriteLine(e.ToString());
				}
			}
			return tablesToExtract;
		}
		public static List<string> GetAllNonDatedIMPTables(NameValueCollection settings)
		{
			List<string> tablesToExtract = new List<string>();
			foreach (string key in settings.AllKeys)
			{
				try
				{
					if (
						(
							key.Contains("IMP") &&
							(settings[key].Contains("*"))
						)
					)
					{
						tablesToExtract.Add(key);
					}
				}
				catch (Exception e)
				{
					Console.WriteLine(e.ToString());
				}
			}
			return tablesToExtract;
		}
		#endregion

		#region Getting IMP Tables
		public static List<string> GetAllPITables(NameValueCollection settings)
		{
			List<string> tablesToExtract = new List<string>();
			foreach (string key in settings.AllKeys)
			{
				try
				{
					if (
						(
							key.Contains("PI.")
						)
					)
					{
						tablesToExtract.Add(key);
					}
				}
				catch (Exception e)
				{
					Console.WriteLine(e.ToString());
				}
			}
			return tablesToExtract;
		}
		public static List<string> GetAllDatedPITables(NameValueCollection settings)
		{
			List<string> tablesToExtract = new List<string>();
			foreach (string key in settings.AllKeys)
			{
				try
				{
					if (
						(
							key.Contains("PI.") &&
							(!settings[key].Contains("*"))
						)
					)
					{
						tablesToExtract.Add(key);
					}
				}
				catch (Exception e)
				{
					Console.WriteLine(e.ToString());
				}
			}
			return tablesToExtract;
		}
		public static List<string> GetAllNonDatedPITables(NameValueCollection settings)
		{
			List<string> tablesToExtract = new List<string>();
			foreach (string key in settings.AllKeys)
			{
				try
				{
					if (
						(
							key.Contains("PI") &&
							(settings[key].Contains("*"))
						)
					)
					{
						tablesToExtract.Add(key);
					}
				}
				catch (Exception e)
				{
					Console.WriteLine(e.ToString());
				}
			}
			return tablesToExtract;
		}
		#endregion

		#region All Tables
		public static List<string> GetAllTables(NameValueCollection settings)
		{
			List<string> tablesToExtract = new List<string>();
			foreach (string key in settings.AllKeys)
			{
				try
				{
					if (
						(
							key.Contains("ITEM") ||
							key.Contains("IMP") ||
							key.Contains("PI_ACTIVITY")
						)
					)
					{
						tablesToExtract.Add(key);
					}
				}
				catch (Exception e)
				{
					Console.WriteLine(e.ToString());
				}
			}
			return tablesToExtract;
		}
		#endregion

		#region Setting Data Destination
		public static string setFinalFilePath(NameValueCollection settings, bool testing, Logger _logger)
		{
			if (testing)
			{
				try
				{
					return settings["TestFileSavePath"];
				}
				catch (Exception e)
				{
					_logger.Error($"Testing file path could not be found. Data will not be saved. Error thrown: {e.ToString()}");
					Console.WriteLine($"Testing file path could not be found. Data will not be saved. Error thrown: {e.ToString()}");
					Program.emailError += $"Testing file path could not be found. Data will not be saved. Error thrown: {e.ToString()}";
					Program.error_occurred = true;
					return null;
				}
			}
			else
			{
				try
				{
					return settings["FinalFilePath"];
				}
				catch (Exception e)
				{
					_logger.Error($"Final file path could not be found. Data will not be saved. Error thrown: {e.ToString()}");
					Console.WriteLine($"Final file path could not be found. Data will not be saved. Error thrown: {e.ToString()}");
					Program.emailError += $"Final file path could not be found. Data will not be saved. Error thrown: {e.ToString()}";
					Program.error_occurred = true;
					return null;
				}
			}
		}
		#endregion

		#region Data Extraction
		public static void CreateTablesWithDateRange(List<DateTime> dates, List<string> tables, BigQueryClient client, NameValueCollection settings, bool testing, Logger _logger)
		{
			// This function is extracts all data from a list of tables that have date ranges.
			string query = null;
			foreach (DateTime currentDate in dates) // Loop through all tables for a single day first, then move to the next day
			{
				foreach (string table in tables)
				{
					_logger.Info($"Extracting {table} for {currentDate:yyyy-MM-dd}");
					Console.WriteLine($"{table}\n{currentDate:yyyy-MM-dd}");
					try
					{
						query = $"SELECT * FROM `schnucks-datalake-prod.{table}` WHERE {settings[table]} = '{currentDate:yyyy-MM-dd}'";
					}
					catch (Exception e) 
					{
						_logger.Warn($"Data failed to load for {table}. Error Thrown: {e.ToString()}");
						Console.WriteLine($"Data failed to load for {table}. Error Thrown: {e.ToString()}");
						Program.emailError += $"Data failed to load for {table}. Error Thrown: {e.ToString()}";
						Program.error_occurred = true;
					}
					if (query != null)
					{
						try
						{
							var results = client.ExecuteQuery(query, null);
							DataTable dt = ConvertToDataTable(results);
							string path = setFinalFilePath(settings, testing, _logger);
							if (path == null)
							{
								_logger.Error($"Data for {table} failed to save to {path}.");
								Program.emailError += $"Data for {table} failed to save to {path}.";
								Program.error_occurred = true;
								Console.WriteLine($"Data for {table} failed to save to {path}.");
							}
							else
							{
								if (testing)
								{
									SaveToCsv(dt, $"{path}/{table.Split('.')[0]}/{table.Split('.')[1]}/{currentDate.Year}/Schnucks_{table}_{currentDate:yyyyMMdd}.csv");
								}
								else
								{
									SaveToCsv(dt, $"{path}/Schnucks_{table}_{currentDate:yyyyMMdd}.csv");
								}
							}
						}
						// This will fail if an issue occurs while running the query
						catch (Exception e)
						{
							_logger.Error($"Data failed to load for {table}. Error Thrown: {e.ToString()}");
							Console.WriteLine($"Data failed to load for {table}. Error Thrown: {e.ToString()}");
							Program.emailError += $"Data failed to load for {table}. Error Thrown: {e.ToString()}";
							Program.error_occurred = true;
						}
					}
					query = null; // Reset query to be null before doing the next table
				}
			}
		}

		public static void CreateTablesWithoutDateRange(List<string> tables, BigQueryClient client, NameValueCollection settings, bool testing, Logger _logger)
		{
			foreach (string table in tables)
			{
				try
				{
					_logger.Info($"Extracting {table}");
					Console.WriteLine(table);
					string query = $"SELECT * FROM `schnucks-datalake-prod.{table}`";
					var results = client.ExecuteQuery(query, null);
					DataTable dt = ConvertToDataTable(results);
					if (testing)
					{
						SaveToCsv(dt, $"{setFinalFilePath(settings, testing, _logger)}/{table.Split('.')[0]}/Schnucks_{table}_{DateTime.Today:yyyyMMdd}.csv");
					}
					else
					{
						SaveToCsv(dt, $"{setFinalFilePath(settings, testing, _logger)}/Schnucks_{table}_{DateTime.Today:yyyyMMdd}.csv");
					}
				}
				/* Check to make sure table names are spelled correctly in App.config. This should only fail 
				 * if there is a misspelling in the names of the tables being extracted in the query*/
				catch(Exception e)
				{
					_logger.Error($"Data failed to load for {table}. Error Thrown: {e.ToString()}");
					Console.WriteLine($"Data failed to load for {table}. Error Thrown: {e.ToString()}");
					Program.emailError += ($"Data failed to load for {table}. Error Thrown: {e.ToString()}");
					Program.error_occurred = true;
				}
			}
		}
		#endregion

		#region Save Results
		static DataTable ConvertToDataTable(BigQueryResults results)
		{
			DataTable dt = new DataTable();
			foreach (var field in results.Schema.Fields)
			{
				dt.Columns.Add(field.Name, typeof(string));
			}

			foreach (var row in results)
			{
				DataRow dataRow = dt.NewRow();
				foreach (var field in results.Schema.Fields)
				{
					dataRow[field.Name] = row[field.Name]?.ToString() ?? "";
				}
				dt.Rows.Add(dataRow);
			}
			return dt;
		}

		public static void SaveToCsv(DataTable dataTable, string filePath)
		{
			Directory.CreateDirectory(Path.GetDirectoryName(filePath));

			using (var writer = new StreamWriter(filePath))
			using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
			{
				foreach (DataColumn column in dataTable.Columns)
				{
					csv.WriteField(column.ColumnName);
				}
				csv.NextRecord();

				foreach (DataRow row in dataTable.Rows)
				{
					foreach (DataColumn column in dataTable.Columns)
					{
						csv.WriteField(row[column]);
					}
					csv.NextRecord();
				}
			}
		}
		#endregion
	}

}