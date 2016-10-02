# CsvDataTable
Works with Csv files.Reads from and writes to DataTable. Writes/Reads one row as CSV row.

Example Read from and save to Csv file:

    var dt = new CsvDataTable();
                dt.ReadFromFile(PathToFile);
  var sort = dt.AsEnumerable().Take(1).Union(dt.AsEnumerable()
                    .Skip(1).OrderBy(g => g["Input ASIN"].ToString())
                    .GroupBy(r => r["ASIN"].ToString())
                    .Select(g => g.FirstOrDefault())
                    .OrderBy(ww => ww["Title"].ToString())
                    .ThenBy(w1 => w1["Size"].ToString())
                    .ThenBy(w1 => w1["Color"].ToString())
                    .ThenBy(w1 => w1["Product Packaging"].ToString())
                    .ThenBy(w1 => w1["Package Quantity"].ToString())
                    .ThenBy(w1 => w1["Flavor"].ToString())
                    .ThenBy(w1 => w1["Team Name"].ToString())
                    .ThenBy(w1 => w1["Pattern Name"].ToString())
                    .ThenBy(w1 => w1["Edition"].ToString()));
   sort.SaveToCsvFile(PathToFile);
