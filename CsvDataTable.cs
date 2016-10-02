using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Csv
{

    public static class CsvExtentions
    {


        public static bool SaveToCsvFile(this IEnumerable<DataRow> rows, string filename, bool append = false,
           Encoding encoding = null, char delimeter = ',', char quote = '"')
        {
            try
            {
                // Like Excel, we'll get the highest column number used,
                // and then write out that many columns for every row
                // var numColumns = rows.First().ItemArray.Length;
                using (var writer = new CsvFileWriter(filename, append, encoding ?? Encoding.UTF8))
                {
                    writer.Delimiter = delimeter;
                    writer.Quote = quote;
                    foreach (DataRow row in rows)
                    {

                        var columns = new List<string>();
                        for (var col = 0; col < row.ItemArray.Length; col++)
                        {
                            var s = String.Empty;
                            if (row[col] != DBNull.Value && row[col] != null)
                            {
                                s = row[col].ToString();
                            }
                            columns.Add(s);
                        }


                        writer.WriteRow(columns);
                    }

                }

                return true;
            }
            catch (Exception e)
            {

            }

            return false;
        }


        public static bool SaveToCsvFile(this DataTable table, string filename, bool append = false,
            Encoding encoding = null, char delimeter = ',', char quote = '"')
        {
            try
            {
                // Like Excel, we'll get the highest column number used,
                // and then write out that many columns for every row
                var numColumns = table.Columns.Count;
                using (var writer = new CsvFileWriter(filename, append, encoding ?? Encoding.UTF8))
                {
                    writer.Delimiter = delimeter;
                    writer.Quote = quote;
                    foreach (DataRow row in table.Rows)
                    {

                        var columns = new List<string>();
                        for (var col = 0; col < numColumns; col++)
                        {
                            var s = String.Empty;
                            if (row[col] != DBNull.Value && row[col] != null)
                            {
                                s = row[col].ToString();
                            }
                            columns.Add(s);
                        }


                        writer.WriteRow(columns);
                    }

                }

                return true;
            }
            catch (Exception e)
            {

            }

            return false;
        }


        public static ParallelLoopResult CsvFileReaderParallel(string path, Action<List<string>> processCsvRow,
            Encoding encoding = null, char delimeter = ',', char quote = '"',
            EmptyLineBehavior emptyLineBehavior = EmptyLineBehavior.NoColumns)
        {
            return Parallel.ForEach(ReadEnumerableCsv(path, encoding, delimeter, quote, emptyLineBehavior),
                processCsvRow);
        }

        public static ParallelLoopResult CsvFileReaderParallel(Stream stream, Action<List<string>> processCsvRow,
            Encoding encoding = null, char delimeter = ',', char quote = '"',
            EmptyLineBehavior emptyLineBehavior = EmptyLineBehavior.NoColumns)
        {
            return Parallel.ForEach(ReadEnumerableCsv(stream, encoding, delimeter, quote, emptyLineBehavior),
                processCsvRow);
        }

        private static IEnumerable<List<string>> ReadEnumerableCsv(string path, Encoding encoding = null,
            char delimeter = ',', char quote = '"', EmptyLineBehavior emptyLineBehavior = EmptyLineBehavior.NoColumns)
        {
            using (
                var csvfile = new CsvFileReader(path, encoding, emptyLineBehavior)
                {
                    Delimiter = delimeter,
                    Quote = quote
                })
            {
                var columns = new List<string>();
                while (csvfile.ReadRow(columns))
                {
                    yield return new List<string>(columns);
                }
            }
        }

        private static IEnumerable<List<string>> ReadEnumerableCsv(Stream stream, Encoding encoding = null,
            char delimeter = ',', char quote = '"', EmptyLineBehavior emptyLineBehavior = EmptyLineBehavior.NoColumns)
        {
            using (
                var csvfile = new CsvFileReader(stream, encoding, emptyLineBehavior)
                {
                    Delimiter = delimeter,
                    Quote = quote
                })
            {
                var columns = new List<string>();
                while (csvfile.ReadRow(columns))
                {
                    yield return new List<string>(columns);
                }
            }
        }

    }

    /// <summary>
    /// Table presents csv data
    /// </summary>
    public class CsvDataTable : DataTable
    {
        public CsvDataTable()
        {

        }


        public bool ReadFromFile(string filename, bool setheaderfromfirstrow = true, Encoding encoding = null, char delimeter = ',', char quote = '"',
           EmptyLineBehavior emptyLineBehavior = EmptyLineBehavior.NoColumns)
        {
            var b = ReadFromFile(filename, encoding, delimeter, quote, emptyLineBehavior);
            try
            {
                for (int i = 0; i < this.Columns.Count; i++)
                {
                    try
                    {
                        this.Columns[i].ColumnName = this.Rows[0][i].ToString();
                    }
                    catch (Exception)
                    {
                    }
                }
            }
            catch (Exception)
            {


            }
            return b;
        }

        /// <summary>
        /// Read csv file
        /// </summary>
        /// <param name="filename">Path to file</param>
        /// <param name="encoding">Encoding</param>
        /// <param name="delimeter">Delimeter between parameters in files</param>
        /// <param name="quote">Brackets around text</param>
        /// <param name="emptyLineBehavior">Determines how empty lines are interpreted when reading CSV files</param>
        /// <returns>true if file read seccessfully</returns>
        public bool ReadFromFile(string filename, Encoding encoding, char delimeter = ',', char quote = '"',
            EmptyLineBehavior emptyLineBehavior = EmptyLineBehavior.NoColumns)
        {
            try
            {
                Rows.Clear();
                var columns = new List<string>();
                using (var reader = new CsvFileReader(filename, encoding ?? Encoding.UTF8, emptyLineBehavior))
                {
                    reader.Delimiter = delimeter;
                    reader.Quote = quote;
                    while (reader.ReadRow(columns))
                    {
                        var cols = Columns.Count;
                        while (Columns.Count < columns.Count)
                        {
                            //Избегаем Null
                            var dc = new DataColumn(String.Format("Column{0}", cols++), typeof(string))
                            {
                                AllowDBNull = false,
                                DefaultValue = string.Empty
                            };
                            Columns.Add(dc);
                        }

                        Rows.Add(columns.ToArray<object>());
                    }
                }
                FileName = filename;

                return true;
            }
            catch (Exception ex)
            {
                //throw new Exception("Error while load csv file",ex);
                //MessageBox.Show(String.Format("Error reading from {0}.\r\n\r\n{1}", filename, ex.Message));
            }

            return false;
        }


        /// <summary>
        /// Read csv file
        /// </summary>
        /// <param name="filename">Path to file</param>
        /// <param name="delimeter"></param>
        /// <param name="encoding">Encoding (null equal Utf-8)</param>
        /// <param name="quote">Brackets around text</param>
        /// <param name="emptyLineBehavior">Determines how empty lines are interpreted when reading CSV files</param>
        /// <returns></returns>
        public bool ReadFromFile(string filename, char delimeter, Encoding encoding = null, char quote = '"',
            EmptyLineBehavior emptyLineBehavior = EmptyLineBehavior.NoColumns)
        {
            return ReadFromFile(filename, encoding, delimeter, quote, emptyLineBehavior);
        }

        public static bool SaveRowToFile(string filename, List<string> row, bool append = false,
            Encoding encoding = null, char delimeter = ',', char quote = '"')
        {
            try
            {
                using (var writer = new CsvFileWriter(filename, append, encoding))
                {
                    writer.Delimiter = delimeter;
                    writer.Quote = quote;


                    var columns = row.Select(o => o ?? string.Empty).ToList();

                    writer.WriteRow(columns);
                    writer.Close();
                }


                return true;
            }
            catch (Exception)
            {
                // MessageBox.Show(String.Format("Error writing to {0}.\r\n\r\n{1}", filename, ex.Message));
            }
            return false;
        }


        public static bool SaveRowToFile(string filename, IDictionary<string, object> row, bool append = false,
            Encoding encoding = null, char delimeter = ',', char quote = '"')
        {
            try
            {
                // Like Excel, we'll get the highest column number used,
                // and then write out that many columns for every row
                var numColumns = row.Count;
                using (var writer = new CsvFileWriter(filename, append, encoding))
                {
                    writer.Delimiter = delimeter;
                    writer.Quote = quote;


                    var columns = new List<string>();
                    foreach (var o in row)
                    {
                        if (o.Value == null)
                        {
                            columns.Add(string.Empty);
                        }
                        else
                        {
                            columns.Add(o.Value.ToString());
                        }
                    }

                    writer.WriteRow(columns);
                }


                return true;
            }
            catch (Exception)
            {
                // MessageBox.Show(String.Format("Error writing to {0}.\r\n\r\n{1}", filename, ex.Message));
            }
            return false;
        }

        public static bool SaveRowToFile(string filename, IDictionary<string, string> row, bool append = false,
            Encoding encoding = null, char delimeter = ',', char quote = '"')
        {
            var rows = new Dictionary<string, object>();
            foreach (var item in row)
            {
                rows[item.Key] = item.Value;
            }
            return SaveRowToFile(filename, rows, append, encoding, delimeter, quote);
        }

        public static bool SaveRowToFile(string filename, DataRow row, bool append = false, Encoding encoding = null,
            char delimeter = ',', char quote = '"')
        {
            try
            {
                // Like Excel, we'll get the highest column number used,
                // and then write out that many columns for every row
                var numColumns = row.ItemArray.Length;
                using (var writer = new CsvFileWriter(filename, append, encoding))
                {
                    writer.Delimiter = delimeter;
                    writer.Quote = quote;


                    var columns = new List<string>();
                    for (var col = 0; col < numColumns; col++)
                    {
                        if (row[col] == null || row[col] == DBNull.Value)
                        {
                            columns.Add(string.Empty);
                        }
                        else
                        {
                            columns.Add(row[col].ToString());
                        }

                    }

                    writer.WriteRow(columns);
                }


                return true;
            }
            catch (Exception)
            {
                // MessageBox.Show(String.Format("Error writing to {0}.\r\n\r\n{1}", filename, ex.Message));
            }
            return false;
        }

        /// <summary>
        /// Write csv file from this table
        /// </summary>
        /// <param name="filename">Path to file</param>
        /// <param name="append">Append to end of file</param>
        /// <param name="encoding">Encoding</param>
        /// <param name="delimeter">Delimeter between parameters in files</param>
        /// <param name="quote">Brackets around text</param>
        /// <returns>true if file save seccessfully</returns>
        public bool SaveToFile(string filename, bool append = false, Encoding encoding = null, char delimeter = ',',
            char quote = '"')
        {
            try
            {
                // Like Excel, we'll get the highest column number used,
                // and then write out that many columns for every row
                var numColumns = Columns.Count;
                using (var writer = new CsvFileWriter(filename, append, encoding ?? Encoding.UTF8))
                {
                    writer.Delimiter = delimeter;
                    writer.Quote = quote;
                    foreach (DataRow row in Rows)
                    {

                        var columns = new List<string>();
                        for (var col = 0; col < numColumns; col++)
                        {

                            var s = string.Empty;
                            if (row[col] != DBNull.Value && row[col] != null)
                            {
                                s = row[col].ToString();
                            }
                            columns.Add(s);
                        }

                        writer.WriteRow(columns);
                    }

                }
                FileName = filename;

                return true;
            }
            catch (Exception)
            {
                // MessageBox.Show(String.Format("Error writing to {0}.\r\n\r\n{1}", filename, ex.Message));
            }
            return false;
        }

        /// <summary>
        /// Name of file
        /// </summary>
        public string FileName { get; private set; }

    }

    /// <summary>
    /// Determines how empty lines are interpreted when reading CSV files.
    /// These values do not affect empty lines that occur within quoted fields
    /// or empty lines that appear at the end of the input file.
    /// </summary>
    public enum EmptyLineBehavior
    {
        /// <summary>
        /// Empty lines are interpreted as a line with zero columns.
        /// </summary>
        NoColumns,

        /// <summary>
        /// Empty lines are interpreted as a line with a single empty column.
        /// </summary>
        EmptyColumn,

        /// <summary>
        /// Empty lines are skipped over as though they did not exist.
        /// </summary>
        Ignore,

        /// <summary>
        /// An empty line is interpreted as the end of the input file.
        /// </summary>
        EndOfFile,
    }

    /// <summary>
    /// Common base class for CSV reader and writer classes.
    /// </summary>
    public abstract class CsvFileCommon
    {
        /// <summary>
        /// These are special characters in CSV files. If a column contains any
        /// of these characters, the entire column is wrapped in double quotes.
        /// </summary>
        protected char[] SpecialChars = new char[] { ',', '"', '\r', '\n' };

        // Indexes into SpecialChars for characters with specific meaning
        private const int DelimiterIndex = 0;
        private const int QuoteIndex = 1;

        /// <summary>
        /// Gets/sets the character used for column delimiters.
        /// </summary>
        public char Delimiter
        {
            get { return SpecialChars[DelimiterIndex]; }
            set { SpecialChars[DelimiterIndex] = value; }
        }

        /// <summary>
        /// Gets/sets the character used for column quotes.
        /// </summary>
        public char Quote
        {
            get { return SpecialChars[QuoteIndex]; }
            set { SpecialChars[QuoteIndex] = value; }
        }
    }




    /// <summary>
    /// Class for reading from comma-separated-value (CSV) files
    /// </summary>
    public class CsvFileReader : CsvFileCommon, IDisposable
    {
        // Private members
        private StreamReader Reader;
        private string _currLine;
        private int _currPos;
        private EmptyLineBehavior EmptyLineBehavior;


        /// <summary>
        /// Initializes a new instance of the CsvFileReader class for the
        /// specified stream.
        /// </summary>
        /// <param name="stream">The stream to read from</param>
        /// <param name="encoding">Encoding</param>
        /// <param name="emptyLineBehavior">Determines how empty lines are handled</param>
        public CsvFileReader(Stream stream, Encoding encoding = null,
            EmptyLineBehavior emptyLineBehavior = EmptyLineBehavior.NoColumns)
        {
            Reader = new StreamReader(stream, encoding ?? Encoding.UTF8);
            EmptyLineBehavior = emptyLineBehavior;
        }

        /// <summary>
        /// Initializes a new instance of the CsvFileReader class for the
        /// specified file path.
        /// </summary>
        /// <param name="path">The name of the CSV file to read from</param>
        /// <param name="encoding">Encoding</param>
        /// <param name="emptyLineBehavior">Determines how empty lines are handled</param>
        public CsvFileReader(string path, Encoding encoding = null,
            EmptyLineBehavior emptyLineBehavior = EmptyLineBehavior.NoColumns)
        {
            Reader = new StreamReader(path, encoding ?? Encoding.UTF8);
            EmptyLineBehavior = emptyLineBehavior;
        }

        /// <summary>
        /// Reads a row of columns from the current CSV file. Returns false if no
        /// more data could be read because the end of the file was reached.
        /// </summary>
        /// <param name="columns">Collection to hold the columns read</param>
        public bool ReadRow(List<string> columns)
        {
            // Verify required argument
            if (columns == null)
                throw new ArgumentNullException("columns");

        ReadNextLine:
            // Read next line from the file
            _currLine = Reader.ReadLine();
            _currPos = 0;
            // Test for end of file
            if (_currLine == null)
                return false;
            // Test for empty line
            if (_currLine.Length == 0)
            {
                switch (EmptyLineBehavior)
                {
                    case EmptyLineBehavior.NoColumns:
                        columns.Clear();
                        return true;
                    case EmptyLineBehavior.Ignore:
                        goto ReadNextLine;
                    case EmptyLineBehavior.EndOfFile:
                        return false;
                }
            }

            // Parse line
            string column;
            var numColumns = 0;
            while (true)
            {
                // Read next column
                if (_currPos < _currLine.Length && _currLine[_currPos] == Quote)
                    column = ReadQuotedColumn();
                else
                    column = ReadUnquotedColumn();
                // Add column to list
                if (numColumns < columns.Count)
                    columns[numColumns] = column;
                else
                    columns.Add(column);
                numColumns++;
                // Break if we reached the end of the line
                if (_currLine == null || _currPos == _currLine.Length)
                    break;
                // Otherwise skip delimiter
                Debug.Assert(_currLine[_currPos] == Delimiter);
                _currPos++;
            }
            // Remove any unused columns from collection
            if (numColumns < columns.Count)
                columns.RemoveRange(numColumns, columns.Count - numColumns);
            // Indicate success
            return true;
        }

        /// <summary>
        /// Reads a quoted column by reading from the current line until a
        /// closing quote is found or the end of the file is reached. On return,
        /// the current position points to the delimiter or the end of the last
        /// line in the file. Note: CurrLine may be set to null on return.
        /// </summary>
        private string ReadQuotedColumn()
        {
            // Skip opening quote character
            Debug.Assert(_currPos < _currLine.Length && _currLine[_currPos] == Quote);
            _currPos++;

            // Parse column
            var builder = new StringBuilder();
            while (true)
            {
                while (_currPos == _currLine.Length)
                {
                    // End of line so attempt to read the next line
                    _currLine = Reader.ReadLine();
                    _currPos = 0;
                    // Done if we reached the end of the file
                    if (_currLine == null)
                        return builder.ToString();
                    // Otherwise, treat as a multi-line field
                    builder.Append(Environment.NewLine);
                }

                // Test for quote character
                if (_currLine[_currPos] == Quote)
                {
                    // If two quotes, skip first and treat second as literal
                    var nextPos = (_currPos + 1);
                    if (nextPos < _currLine.Length && _currLine[nextPos] == Quote)
                        _currPos++;
                    else
                        break; // Single quote ends quoted sequence
                }
                // Add current character to the column
                builder.Append(_currLine[_currPos++]);
            }

            if (_currPos >= _currLine.Length)
            {
                return builder.ToString();
            }
            // Consume closing quote
            Debug.Assert(_currLine[_currPos] == Quote);
            _currPos++;
            // Append any additional characters appearing before next delimiter
            builder.Append(ReadUnquotedColumn());
            // Return column value
            return builder.ToString();
        }

        /// <summary>
        /// Reads an unquoted column by reading from the current line until a
        /// delimiter is found or the end of the line is reached. On return, the
        /// current position points to the delimiter or the end of the current
        /// line.
        /// </summary>
        private string ReadUnquotedColumn()
        {
            var startPos = _currPos;
            _currPos = _currLine.IndexOf(Delimiter, _currPos);
            if (_currPos == -1)
                _currPos = _currLine.Length;
            if (_currPos > startPos)
                return _currLine.Substring(startPos, _currPos - startPos);
            return String.Empty;
        }

        // Propagate Dispose to StreamReader
        public void Dispose()
        {
            Reader.Dispose();
        }
    }

    /// <summary>
    /// Class for writing to comma-separated-value (CSV) files.
    /// </summary>
    public class CsvFileWriter : CsvFileCommon, IDisposable
    {
        // Private members
        private StreamWriter Writer;
        private string _oneQuote;
        private string _twoQuotes;
        private string _quotedFormat;

        /// <summary>
        /// Initializes a new instance of the CsvFileWriter class for the
        /// specified stream.
        /// </summary>
        /// <param name="stream">The stream to write to</param>
        /// <param name="encoding"></param>
        public CsvFileWriter(Stream stream, Encoding encoding = null)
        {
            Writer = new StreamWriter(stream, encoding ?? Encoding.UTF8);
        }

        /// <summary>
        /// Initializes a new instance of the CsvFileWriter class for the
        /// specified file path.
        /// </summary>
        /// <param name="path">The name of the CSV file to write to</param>
        /// <param name="append">Append to end of file</param>
        /// <param name="encoding">Encoding, default Encoding.Utf8</param>
        public CsvFileWriter(string path, bool append = false, Encoding encoding = null)
        {
            //if (encoding == null) encoding = Encoding.UTF8;
            Writer = new StreamWriter(path, append, encoding ?? Encoding.UTF8);
        }

        /// <summary>
        /// Writes a row of columns to the current CSV file.
        /// </summary>
        /// <param name="columns">The list of columns to write</param>
        public void WriteRow(List<string> columns)
        {
            // Verify required argument
            if (columns == null)
                throw new ArgumentNullException("columns");

            // Ensure we're using current quote character
            if (_oneQuote == null || _oneQuote[0] != Quote)
            {
                _oneQuote = String.Format("{0}", Quote);
                _twoQuotes = String.Format("{0}{0}", Quote);
                _quotedFormat = String.Format("{0}{{0}}{0}", Quote);
            }

            // Write each column
            for (var i = 0; i < columns.Count; i++)
            {
                // Add delimiter if this isn't the first column
                if (i > 0)
                    Writer.Write(Delimiter);
                // Write this column
                if (columns[i].IndexOfAny(SpecialChars) == -1)
                    Writer.Write(columns[i]);
                else
                    Writer.Write(_quotedFormat, columns[i].Replace(_oneQuote, _twoQuotes));
            }
            Writer.WriteLine();
        }

        // Propagate Dispose to StreamWriter
        public void Dispose()
        {
            Writer.Dispose();
        }

        public void Close()
        {
            Writer.Close();
        }
    }
}