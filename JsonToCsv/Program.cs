using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Newtonsoft.Json;

namespace JsonToCsv
{
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Reflection.Emit;

    using TinyCsvParser;
    using TinyCsvParser.Mapping;

    public class Program
    {
        private StringBuilder kibanaCsv;

        private StringBuilder appcenterCsv;

        private HashSet<MsgItem> msgItems;

        private HashSet<LogItem> logItems;

        private static string EventName = "Enquiry push message received";

        private static string KibanaEventName = "Sending Android notification with jsonPayload:";

        static List<LogItem> _myList;

        [SuppressMessage("StyleCop.CSharp.DocumentationRules", "SA1600:ElementsMustBeDocumented", Justification = "Reviewed. Suppression is OK here.")]
        public static void Main(string[] args)
        {
            IEnumerable<MsgItem> kibana = null;
            IEnumerable<Item> appCenter = null;
            Console.WriteLine("Running PushNotification script");

            List<CsvMappingResult<DbItem>> dbItems = null;
            try
            {
                dbItems = PopulateDbItems("Logs/DbItems", "dbItems.csv");
                appCenter = RecursiveFolderTraverse("Logs/Appcenter");
                kibana = KibanaFolderSearch("Logs/Kibana");
            }
            catch (Exception ex)
            {
                if (ex is DirectoryNotFoundException || ex is FileNotFoundException)
                {
                    Console.WriteLine($"{ex.Data["Type"]} {ex.Data["FolderName"]} was not found. Copy data to folder and re-run the script");
                    Directory.CreateDirectory(ex.Data["FolderName"].ToString());

                    Console.ReadLine();
                    return;
                }
            }

            _myList = appCenter.Join(
                kibana, x => x.Eid, x => x.eid,
                (x, y) =>
                    {
                        DbItem enq = null;
                        for (var i = 0; i < dbItems.Count; i++)
                        {
                            if (dbItems[i].Result.eid == x.Eid)
                            {
                                enq = dbItems[i].Result;
                                break;
                            }
                        }

                        if (enq != null)
                        {
                            //var enqPublished = dbItems.Find(z => z.Result.enquiryPublished == x.Eid).Result;

                            return new LogItem()
                                       {
                                           appCenterTimestamp = x.Timestamp,
                                           appVersion = x.AppVersion,
                                           dbEnqPublished = enq.enquiryPublished.ToUniversalTime(),
                                           eid = x.Eid,
                                           kibanaTimestamp = y.Timestamp,
                                           stage1TimeTaken = y.Timestamp - enq.enquiryPublished.ToUniversalTime(),
                                           stage2TimeTaken = x.Timestamp - y.Timestamp
                                       };
                        }

                        return null;
                    }).ToList();
            
            if (_myList.Count > 0)
            {
                using (var fileWrite = new StreamWriter("Logfile.csv"))
                {
                    var newList = _myList.Where(x => x != null && x.appCenterTimestamp.CompareTo(x.kibanaTimestamp) > 0 && x.kibanaTimestamp.CompareTo(x.dbEnqPublished) > 0);
                    var newListCount = (double)newList.ToList().Count;

                    var mySortedList = _myList.Where(x => x != null).OrderBy(o => o.stage1TimeTaken).ToList();
                    var mySortedList2 = _myList.Where(x => x != null).OrderBy(o => o.stage2TimeTaken).ToList();
                    
                    var median = mySortedList[mySortedList.Count / 2];

                    var timeTakenStage1 = mySortedList.Select(x => x.stage1TimeTaken);
                    var average1 = timeTakenStage1.Average(x => x.TotalSeconds);

                    var meanStage1 =
                        new TimeSpan(
                            0, 0, 0, (int)average1);
                    //var meanStage1 = meanStage1Tick;
                    //var meanStage2 = new TimeSpan(Convert.ToInt64(mySortedList.Select(x => x.stage2TimeTaken).Average(timeSpan => timeSpan.Ticks)));

                    var timeTakenStage2 = mySortedList.Select(x => x.stage1TimeTaken);
                    var average2 = timeTakenStage2.Average(x => x.TotalSeconds);

                    var meanStage2 =
                        new TimeSpan(
                            0, 0, 0, (int)average2);

                    var fiveMinValStage1 = GetPercentage(5, newListCount, 1);
                    var tenMinValStage1 = GetPercentage(10, newListCount, 1);
                    var fifteenMinValStage1 = GetPercentage(15, newListCount, 1);

                    var fiveMinValStage2 = GetPercentage(5, newListCount, 2);
                    var tenMinValStage2 = GetPercentage(10, newListCount, 2);
                    var fifteenMinValStage2 = GetPercentage(15, newListCount, 2);

                    fileWrite.WriteLine($"Median value Stage 1:,{median.stage1TimeTaken}");
                    fileWrite.WriteLine($"Mean value Stage 1:,{meanStage1}");
                    fileWrite.WriteLine($"Over 5 min:,{fiveMinValStage1}");
                    fileWrite.WriteLine($"Over 10 min:,{tenMinValStage1}");
                    fileWrite.WriteLine($"Over 15 min:,{fifteenMinValStage1}");

                    fileWrite.WriteLine($"Median value Stage 2:,{mySortedList2[mySortedList2.Count /2].stage2TimeTaken}");
                    fileWrite.WriteLine($"Mean value Stage 2:,{meanStage2}");
                    fileWrite.WriteLine($"Over 5 min:,{fiveMinValStage2}");
                    fileWrite.WriteLine($"Over 10 min:,{tenMinValStage2}");
                    fileWrite.WriteLine($"Over 15 min:,{fifteenMinValStage2}");
                
                    fileWrite.WriteLine();
                    fileWrite.WriteLine($"EID,APPVERSION,DB TIMESTAMP,KIBANA TIMESTAMP,STAGE 1 TIME TAKEN,APPCENTER TIMESTAMP,STAGE 2 TIME TAKEN");
                    
                    foreach (var x in newList)
                    {
                        fileWrite.WriteLine(
                            $"{x.eid},{x.appVersion},{x.dbEnqPublished},{x.kibanaTimestamp},{x.stage1TimeTaken},{x.appCenterTimestamp},{x.stage2TimeTaken}");
                    }

                    Console.WriteLine($"Median value Stage 1:,{median.stage1TimeTaken}");
                    Console.WriteLine($"Mean value Stage 1:,{meanStage1}");
                    Console.WriteLine($"Over 5 min:,{fiveMinValStage1}");
                    Console.WriteLine($"Over 10 min:,{tenMinValStage1}");
                    Console.WriteLine($"Over 15 min:,{fifteenMinValStage1}");

                    Console.WriteLine($"Median value Stage 2:,{median.stage2TimeTaken}");
                    Console.WriteLine($"Mean value Stage 2:,{meanStage2}");
                    Console.WriteLine($"Over 5 min:,{fiveMinValStage2}");
                    Console.WriteLine($"Over 10 min:,{tenMinValStage2}");
                    Console.WriteLine($"Over 15 min:,{fifteenMinValStage2}");
                    Console.WriteLine("Press enter to quit");
                    Console.ReadLine();

                    return;
                }
            }

            Console.WriteLine("No data to summarize. Copy data to folder and re-run the script");
            Console.ReadKey();
        }

        private static string GetPercentage(int minInterval, double  origListCount, int stage)
        {
            double overIntervalMinutes;

            if (stage == 1)
            {
                overIntervalMinutes = (double)_myList.Where(x => x != null && x.stage1TimeTaken.Minutes >= minInterval).ToList().Count;
            }
            else
            {
                overIntervalMinutes = (double)_myList.Where(x => x != null && x.stage2TimeTaken.Minutes >= minInterval).ToList().Count;
            }
            
            var percOverIntervalLimit = (overIntervalMinutes / origListCount).ToString("0.0%").Replace(",", ".");

            return percOverIntervalLimit;
        }

        private static List<CsvMappingResult<DbItem>> PopulateDbItems(string folderName, string fileName)
        {
            try
            {
                Directory.Exists(folderName);
                CsvParserOptions csvParserOptions = new CsvParserOptions(true, ',');
                DbItemMapping csvMapper = new DbItemMapping();
                CsvParser<DbItem> csvParser = new CsvParser<DbItem>(csvParserOptions, csvMapper);

                return csvParser.ReadFromFile($"{folderName}/{fileName}", Encoding.ASCII).ToList();
            }
            catch (DirectoryNotFoundException ex)
            {
                ex.Data.Add("Type", "Directory");
                ex.Data.Add("FolderName", folderName);
                throw;
            }
            catch (FileNotFoundException ex)
            {
                ex.Data.Add("Type", "File");
                ex.Data.Add("FolderName", fileName);
                throw;
            }
        }

        [SuppressMessage("StyleCop.CSharp.DocumentationRules", "SA1600:ElementsMustBeDocumented", Justification = "Reviewed. Suppression is OK here.")]
        private static IEnumerable<Item> RecursiveFolderTraverse(string folderName)
        {
            try
            {
                var files = Directory.GetFiles(folderName, "*", SearchOption.AllDirectories).Select(File.ReadAllText);

                return files.SelectMany(JsonConvert.DeserializeObject<List<Item>>)
                    .Where(x => x.EventName.Contains(EventName)).Select(
                        x => new Item()
                                 {
                                     AppVersion = x.AppVersion,
                                     EventName = x.EventName,
                                     Eid =
                                         Guid.Parse(JsonConvert.DeserializeObject<Item.MyId>(x.Properties).Eid),
                                     Properties = x.Properties,
                                     Timestamp = x.Timestamp
                                 }).ToList();
            }
            catch (DirectoryNotFoundException ex)
            {
                ex.Data.Add("Type", "Directory");
                ex.Data.Add("FolderName", folderName);
                throw;
            }
        }

        [SuppressMessage("StyleCop.CSharp.DocumentationRules", "SA1600:ElementsMustBeDocumented", Justification = "Reviewed. Suppression is OK here.")]
        private static IEnumerable<MsgItem> KibanaFolderSearch(string folderName)
        {
            try
            {
                var files = Directory.GetFiles(folderName, "*", SearchOption.AllDirectories)
                    .Select(File.ReadAllText).SelectMany(x => x.Split('\n'));

                return files.Select(JsonConvert.DeserializeObject<MsgItem>).Where(x => x != null && x.Message.Contains(KibanaEventName))
                    .Select(x => new MsgItem()
                                     {
                                         Message = x.Message,
                                         Timestamp = x.Timestamp,
                                         eid = Guid.Parse(x.Message.Substring(x.Message.IndexOf("eid", StringComparison.Ordinal) + 6, 36))
                                     })
                    .ToList();

            }
            catch (DirectoryNotFoundException ex)
            {
                ex.Data.Add("Type", "Directory");
                ex.Data.Add("FolderName", folderName);
                throw;
            }
        }
    }

    public class DbItem
    {
        public Guid eid { get; set; }
        
        public DateTime enquiryPublished { get; set; }
    }

    public class DbItemMapping : CsvMapping<DbItem>
    {
        public DbItemMapping()
            : base()
        {
            MapProperty(0, x => x.eid);
            MapProperty(1, x => x.enquiryPublished);
        }
    }

    public class MsgItem
    {
        public string Message;

        public Guid eid;

        public DateTime Timestamp;
    }

    public class LogItem
    {
        public Guid ? eid;
        public Guid mid;
        public DateTime dbEnqPublished;
        public DateTime kibanaTimestamp;
        public DateTime appCenterTimestamp;

        public TimeSpan stage1TimeTaken;
        public TimeSpan stage2TimeTaken;
        public string appVersion;
    }

    /// <summary>
    /// The item.
    /// </summary>
    public class Item
    {
        /// <summary>
        /// The millis.
        /// </summary>
        public DateTime Timestamp;

        /// <summary>
        /// The stamp.
        /// </summary>
        public string EventName;

        public Guid ? Eid;
        public string Properties;
        public string AppVersion;

        public class MyId
        {
            public string Eid;
        }
    }
}
