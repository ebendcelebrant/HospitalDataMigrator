using CsvHelper;
using System;
using System.Globalization;
using System.IO;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Threading.Tasks;

namespace HospitalDataMigrator
{
    class Program
    {
        public const int DataTableLimit = 5000000;
        public static Message message;
        public static string batchName;
        static void Main(string[] args)
        {
            GlobalInit();
            Console.WriteLine("Hello!");
            Console.WriteLine("Please provide file location for the data:");
            string path = Console.ReadLine();
            message = ValidateFile(path);
            if (message.MessageType != Message.MessageTypes.Error)
            {
                ParallelDataLoad(path);

                Console.WriteLine("Data upload successful! Commencing data migration...");
                message = RunMigration();

                Console.WriteLine("Migration run completed! Status:{1} Message: {0}", message.MessageBody, message.MessageType.ToString());
            }
            else
            {
                Console.WriteLine("There was an error. Details: {0}", message.MessageBody);
            }

            Console.ReadLine();
        }

        public static void GlobalInit()
        {
            batchName = String.Format("HospitalUpload{0}{1}{2}{3}{4}{5}{6}", DateTime.Now.Day, DateTime.Now.Month,
                                                           DateTime.Now.Year, DateTime.Now.Hour, DateTime.Now.Minute,
                                                           DateTime.Now.Second, DateTime.Now.Millisecond);
            message = new Message();

        }

        public static Message RunMigration()
        {
            using (SqlConnection conn = new SqlConnection("Data Source =.; Initial Catalog = TSolutionAssessment; Integrated Security = True"))
            {
                using (SqlCommand cmd = new SqlCommand("MigrateDataDump", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@batchName", batchName);
                    conn.Open();
                    SqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    if (!dr.Read())
                    {
                        conn.Close();
                        return new Message("Error in migration!", Message.MessageTypes.Error);
                    }
                    else
                    {
                        var response = new Message(dr[0].ToString(), (Message.MessageTypes)int.Parse(dr[1].ToString()));
                        conn.Close();
                        return response;
                    }
                }
            }
        }

        private static List<DataTable> ProcessCSV(string path)
        {
            try
            {

                TextReader reader = new StreamReader(path);
                var csvReader = new CsvReader(reader, CultureInfo.InvariantCulture);

                var properties = typeof(DataDump).GetProperties();

                List<DataTable> dataDumpTables = new List<DataTable>();

                var dataDumpTable = new DataTable();
                foreach (var info in properties)
                    dataDumpTable.Columns.Add(info.Name, Nullable.GetUnderlyingType(info.PropertyType)
                       ?? info.PropertyType);

                int i = 0;
                //csvReader.ReadHeader();
                csvReader.Read();
                csvReader.ReadHeader();
                while (csvReader.Read())
                {
                    if (i == DataTableLimit)
                    {
                        dataDumpTables.Add(dataDumpTable);
                        dataDumpTable = new DataTable();
                        foreach (var info in properties)
                            dataDumpTable.Columns.Add(info.Name, Nullable.GetUnderlyingType(info.PropertyType)
                               ?? info.PropertyType);
                    }
                    var row = dataDumpTable.NewRow();
                    foreach (DataColumn column in dataDumpTable.Columns)
                    {
                        if (column.ColumnName == "BatchName")
                        {
                            row["BatchName"] = batchName;
                            continue;
                        }
                        row[column.ColumnName] = csvReader.GetField(column.DataType, column.ColumnName);
                    }
                    dataDumpTable.Rows.Add(row);
                    i++;
                }
                dataDumpTables.Add(dataDumpTable);
                return dataDumpTables;
                //var records = csvReader.GetRecords<DataDump>();
            }
            catch (Exception ex)
            {
                message.MessageType = Message.MessageTypes.Error;
                message.MessageBody = ex.Message;

                return new List<DataTable>();
            }
        }
        public static void ParallelDataLoad(string path)
        {
            // ProcessCSV() is a method that returns a List<> of DataTables.
            // Each DataTable is populated with 5 million rows.
            List<DataTable> tables = ProcessCSV(path);

            if (tables.Count <=0)
            {
                Console.WriteLine("No data to process. {1} {0}", message.MessageType, message.MessageBody);
                return;
            }

            // For each of the DataTables, bulk load task to run in parallel
            Parallel.ForEach(tables, table =>
            {
                BulkLoadData(table);
            }
            );
        }

        public static void BulkLoadData(DataTable dt)
        {
            var properties = typeof(DataDump).GetProperties();
            using (SqlConnection conn = new SqlConnection("Data Source =.; Initial Catalog = TSolutionAssessment; Integrated Security = True"))
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null))
            {
                bulkCopy.DestinationTableName = "DataDump";
                bulkCopy.BulkCopyTimeout = 60;

                foreach (var info in properties)
                    bulkCopy.ColumnMappings.Add(info.Name, info.Name);
                conn.Open();
                bulkCopy.WriteToServer(dt);
                bulkCopy.Close();
            }
        }

        public static Message ValidateFile(string path)
        {
            //Implement Guard
            if (string.IsNullOrEmpty(path))
            {
                return new Message("Error. Empty File Path Provided!!", Message.MessageTypes.Error);
            }
            else
            {
                //Verify File Path Exists
                FileInfo upload = new FileInfo(path);
                if (!upload.Exists)
                {
                    return new Message("Error. File does not exists!", Message.MessageTypes.Error);
                }
                return new Message("", Message.MessageTypes.Success);
            }
        }
    }

    public class Message
    {
        public Message() { }
        public Message(string messageBody, MessageTypes messageType)
        {
            MessageBody = messageBody;
            MessageType = messageType;
        }

        public enum MessageTypes : byte
        {
            Error = 1,
            Success
        }
        public string MessageBody { get; set; }
        public MessageTypes MessageType { get; set; }
    }

    public class DataDump
    {
        public string PatientName { get; set; }
        public string BloodGroup { get; set; }
        public string PhoneNumber { get; set; }
        public string TreatmentDetails { get; set; }
        public DateTime DateOfBirth { get; set; }
        public int Age { get; set; }
        public DateTime Date { get; set; }
        public string DoctorName { get; set; }
        public decimal Charge { get; set; }
        public string BatchName { get; set; }

    }
}
