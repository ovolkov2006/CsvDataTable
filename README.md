//# CsvDataTable
//Works with Csv files.Reads from and writes to DataTable. Writes/Reads one row as CSV row.
var csv=new CsvDataTable();
csv.ReadFromFile("file.csv");
//work with table
csv.SaveToFile("newfile.csv");
csv.Dispose();



CsvExtentions.CsvFileReaderParallel("input.csv", columns =>
                {
                    Interlocked.Increment(ref counter);
                    lock (_locker)
                    {
                        CsvDataTable.SaveRowToFile("test_new.csv", columns, true);    
                    }
                });
