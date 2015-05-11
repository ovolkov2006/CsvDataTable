//# CsvDataTable
//Works with Csv files.Reads from and writes to DataTable. Writes/Reads one row as CSV row.
var csv=new CsvDataTable();
csv.ReadFromFile("file.csv");
//work with table
csv.SaveToFile("newfile.csv");
csv.Dispose();
