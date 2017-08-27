using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace PgDbfLib
{
    public enum PgVersion
    {
        Pg8_2_And_Newer,
        Pg8_1_And_Older
    }

    public class DbfExporter
    {
        /// <summary>
        /// The name of the dbf file to export. All non-alphanumeric characters are replaced with "_"
        /// </summary>
        public string DbfFileName
        {
            get
            {
                return dbfFileName;
            }
            set
            {
                dbfFileName = value;
                TableName = Path.GetFileNameWithoutExtension(value);
            }
        }
        private string dbfFileName;
        /// <summary>
        /// The name of the table to insert to. Defaults to <see cref="DbfFileName"/> sans extension.
        /// </summary>
        public string TableName
        {
            get
            {
                return tableName;
            }
            set
            {
                Regex filter = new Regex(@"\W+");
                tableName = filter.Replace(value, "_");
            }
        }
        private string tableName;
        /// <summary>
        /// Indicates whether to truncate an existing table. Setting this to <value>true</value> will set <see cref="CreateTable"/> and <see cref="DropTable"/> to <value>false</value>
        /// Defaults to <value>false</value>.
        /// </summary>
        public bool TruncateTable
        {
            get
            {
                return truncateTable;
            }
            set
            {
                truncateTable = value;
                if (value)
                {
                    createTable = false;
                    DropTable = false;
                }
            }
        }
        private bool truncateTable;
        /// <summary>
        /// Indicates whether to create a new table. Setting this to <value>true</value> sets <see cref="TruncateTable"/> to <value>false</value>.
        /// Defaults to <value>true</value>.
        /// </summary>
        public bool CreateTable
        {
            get
            {
                return createTable;
            }
            set
            {
                createTable = value;
                if (value)
                {
                    truncateTable = false;
                }
            }
        }
        private bool createTable;
        /// <summary>
        /// Indicates whether an existing table with the same name as <see cref="TableName"/> should be dropped.
        /// Setting this to <value>true</value> sets <see cref="CreateTable"/> to <value>true</value> and <see cref="TruncateTable"/> to <value>false</value>.
        /// Defaults to <value>true</value>.
        /// </summary>
        public bool DropTable
        {
            get
            {
                return dropTable;
            }
            set
            {
                dropTable = value;
                if (value)
                {
                    CreateTable = true;
                }
            }
        }
        private bool dropTable;
        /// <summary>
        /// The version of the database to insert into. Defaults to <value>Pg8_2_And_Newer</value>.
        /// </summary>
        public PgVersion PGSqlVersion { get; set; }
        /// <summary>
        /// Indicates whether to convert numeric values to text. Defaults to <value>false</value>.
        /// </summary>
        public bool ConvertNumericToText { get; set; }
        /// <summary>
        /// Indicates whether to convert boolean values to varchar.  Deafults to <value>false</value>.
        /// </summary>
        public bool ConvertBoolToVarChar { get; set; }
        /// <summary>
        /// Indicates whether to wrap the sql dump in a transaction. Defaults to <value>true</value>.
        /// </summary>
        public bool WrapInTransaction { get; set; }
        /// <summary>
        /// The columns to include in the dump.  If none are specified, all columns will be included.
        /// </summary>
        public List<string> IncludedColumns { get; private set; }
        /// <summary>
        /// Holds any columns that are to be renamed.
        /// </summary>
        public Dictionary<string, string> ColumnRenames { get; private set; }

        private long RecordCount;
        private int SkipBytes;
        private int FieldArraySize;
        private int FieldCount;
        private int MemoBlockSize;
        private FileStream DbfFile;
        private FileStream MemoFile;
        private List<DbfColumn> Columns;
        private static int HeaderSize = 32;
        private static int FieldDefinitionSize = 32;
        private static int FieldNameLength = 11;
        private static int FieldTypeOffset = 11;
        private static int FieldLengthOffset = 16;
        private static int FieldDecimalsOffset = 17;
        private static string[] ReservedWords = new string[]
        {
            "all",
            "analyse",
            "analyze",
            "and",
            "any",
            "array",
            "as",
            "asc",
            "asymmetric",
            "both",
            "case",
            "cast",
            "check",
            "collate",
            "column",
            "constraint",
            "create",
            "current_catalog",
            "current_date",
            "current_role",
            "current_time",
            "current_timestamp",
            "current_user",
            "default",
            "deferrable",
            "desc",
            "distinct",
            "do",
            "else",
            "end",
            "except",
            "false",
            "fetch",
            "for",
            "foreign",
            "from",
            "grant",
            "group",
            "having",
            "in",
            "initially",
            "intersect",
            "into",
            "leading",
            "limit",
            "localtime",
            "localtimestamp",
            "new",
            "not",
            "null",
            "off",
            "offset",
            "old",
            "on",
            "only",
            "or",
            "order",
            "placing",
            "primary",
            "references",
            "returning",
            "select",
            "session_user",
            "some",
            "symmetric",
            "table",
            "then",
            "to",
            "trailing",
            "true",
            "union",
            "unique",
            "user",
            "using",
            "variadic",
            "when",
            "where",
            "window",
            "with",
            "",
        };

        /// <summary>
        /// Initializes the DbfExporter
        /// </summary>
        public DbfExporter()
        {
            truncateTable = false;
            createTable = true;
            dropTable = true;
            ConvertNumericToText = false;
            ConvertBoolToVarChar = false;
            WrapInTransaction = true;
            PGSqlVersion = PgVersion.Pg8_2_And_Newer;
            IncludedColumns = new List<string>();
            ColumnRenames = new Dictionary<string, string>();
            Columns = new List<DbfColumn>();
        }

        /// <summary>
        /// Initializes the DbfExporter
        /// </summary>
        /// <param name="dbfFileName">The name of the Dbf file to export</param>
        public DbfExporter(string dbfFileName) : this()
        {
            DbfFileName = dbfFileName;
        }

        /// <summary>
        /// Creates a PostGresql dump of the indicated dbf file
        /// </summary>
        /// <returns>Returns the script as an <see cref="IEnumerable{T}"/> where T is a <see cref="string"/>.</returns>
        public IEnumerable<string> GetPgScript()
        {
            ReadHeader();
            if (WrapInTransaction)
            {
                yield return "BEGIN;";
            }
            if (DropTable)
            {
                yield return $"SET statement_timeout = 60000; DROP TABLE {(PGSqlVersion == PgVersion.Pg8_2_And_Newer ? "IF EXISTS " : string.Empty)}{TableName};" +
                    "  SET statement_timeout=0;";
            }
            string columnString = GetColumnString();
            yield return CreateTable ? $"CREATE TABLE {TableName} ({columnString});" :
                columnString;
            if (TruncateTable)
            {
                yield return $"TRUNCATE TABLE {TableName};";
            }
            yield return $"COPY {TableName} FROM STDIN";
            foreach (string row in GetRows())
            {
                yield return row;
            }
            yield return @"\.";
            if (WrapInTransaction)
            {
                yield return "COMMIT;";
            }
        }

        private void ReadHeader()
        {
            if (!File.Exists(DbfFileName))
            {
                throw new Exception("The Dbf File does not exist or is inaccessible");
            }
            DbfFile = new FileStream(dbfFileName, FileMode.Open, FileAccess.Read);
            byte[] header = new byte[HeaderSize];
            DbfFile.Read(header, 0, HeaderSize);
            if (header[0] == 0x30)
            {
                SkipBytes = 263;
            }
            RecordCount = BitConverter.ToUInt32(header, 4);
            int headerLength = BitConverter.ToUInt16(header, 8);
            FieldArraySize = headerLength - HeaderSize - SkipBytes - 1;
            if (FieldArraySize % FieldDefinitionSize == 1)
            {
                ++SkipBytes;
                --FieldArraySize;
            }
            FieldCount = FieldArraySize / FieldDefinitionSize;
        }

        /// <summary>
        /// Gets all the Dbf Rows
        /// </summary>
        /// <returns>Returns the rows of a Dbf as an <see cref="IEnumerable{T}"/> where T is a <see cref="string"/></returns>
        public IEnumerable<string> GetRows()
        {
            foreach (IEnumerable<string> row in GetRowFields())
            {
                yield return string.Join("\t", row);
            }
        }

        /// <summary>
        /// Gets all the rows and their fields from the Dbf
        /// </summary>
        /// <remarks>Because DbfExporter is forward-reading, calling this method multiple times will throw an exception.</remarks>
        /// <returns>Returns the rows and individual fields of a Dbf as an <see cref="IEnumerable{T}"/> of <see cref="IEnumerable{T}"/> where T is a <see cref="string"/>"/></returns>
        public IEnumerable<IEnumerable<string>> GetRowFields()
        {
            if (RecordCount == 0)
            {
                GetColumns();
            }
            if (DbfFile == null)
            {
                throw new Exception("Rows have already been read");
            }
            List<string> row;
            int rowLength = Columns.Sum(c => c.Length);
            char[] stringRow = new char[rowLength];
            byte[] rawRow = new byte[rowLength];
            char deleted;
            for (int i = 0; i < RecordCount; ++i)
            {
                deleted = Convert.ToChar(DbfFile.ReadByte());
                if (deleted == '*')
                {
                    DbfFile.Seek(rowLength, SeekOrigin.Current);
                    continue;
                }
                else
                {
                    DbfFile.Read(rawRow, 0, rowLength);
                    row = new List<string>();
                    foreach (var field in Columns)
                    {
                        if (field.Export)
                        {
                            row.Add(field.ConvertRawValue(rawRow));
                        }
                    }
                    yield return row;
                }
            }
            if (MemoFile != null)
            {
                MemoFile.Close();
                MemoFile.Dispose();
            }
            DbfFile.Close();
            DbfFile.Dispose();
        }

        /// <summary>
        /// Gets the column names
        /// </summary>
        /// <returns>Returns the columns of a Dbf as a single <see cref="string"/></returns>
        public string GetColumnString()
        {
            return string.Join(",", GetColumns());
        }

        /// <summary>
        /// Gets the column names
        /// </summary>
        /// <remarks>Because DbfExporter is forward-reading, calling this method multiple times or calling it after <see cref="GetRowFields"/> will throw an exception.</remarks>
        /// <returns>Returns the columns of a Dbf as a <see cref="IEnumerable{T}"/> where T is a <see cref="string"/></returns>
        public IEnumerable<string> GetColumns()
        {
            if (FieldCount == 0)
            {
                ReadHeader();
            }
            if (Columns.Count > 0)
            {
                throw new Exception("Column names have already been read");
            }
            ColumnRenames = ColumnRenames.ToDictionary(d => d.Key.ToUpper(), d => d.Value);
            IncludedColumns = IncludedColumns.Select(s => s.ToUpper()).ToList();
            bool addColumn = IncludedColumns.Count == 0;
            List<string> columnNames = new List<string>();
            byte[] columnHeader = new byte[FieldArraySize];
            DbfFile.Read(columnHeader, 0, FieldArraySize);
            int baseOffset;
            int numericDecimal;
            int columnOffset = 0;
            string pgColumnType;
            for (int i = 0; i < FieldCount; ++i)
            {
                baseOffset = i*FieldDefinitionSize;
                byte[] nameBytes = new byte[FieldNameLength];
                Array.Copy(columnHeader, baseOffset, nameBytes, 0, FieldNameLength);
                string columnName = Encoding.ASCII.GetString(nameBytes).Replace("\0", string.Empty);
                if (ColumnRenames.ContainsKey(columnName))
                {
                    columnName = ColumnRenames[columnName];
                }
                if (ReservedWords.Contains(columnName))
                {
                    int increment = 0;
                    while (IncludedColumns.Contains($"{columnName}_{++increment}"));
                    columnName = $"{columnName}_{increment}";
                }
                if (addColumn)
                {
                    IncludedColumns.Add(columnName);
                }
                var dbfColumnType = Convert.ToChar(columnHeader[baseOffset + FieldTypeOffset]);
                DbfColumn column = dbfColumnType == 'M' ? new MemoColumn() : new DbfColumn();
                column.Export = IncludedColumns.Contains(columnName) || addColumn;
                column.Type = dbfColumnType;
                column.Length = columnHeader[baseOffset + FieldLengthOffset];
                column.Offset = columnOffset;
                columnOffset += column.Length;
                Columns.Add(column);
                if (CreateTable)
                {
                    pgColumnType = "TEXT";
                    if (column.Type == 'N' || column.Type == 'F')
                    {
                        if (ConvertNumericToText)
                        {
                            pgColumnType = "TEXT";
                        }
                        else
                        {
                            numericDecimal = columnHeader[baseOffset + FieldDecimalsOffset];
                            pgColumnType = numericDecimal == 0 ?
                                $"NUMERIC({column.Length})" :
                                $"NUMERIC({column.Length},{numericDecimal})";
                        }
                    }
                    else if (column.Type == 'L')
                    {
                        if (ConvertBoolToVarChar)
                        {
                            column.Type = 'C';
                            pgColumnType = "VARCHAR(1)";
                        }
                        else
                        {
                            pgColumnType = "BOOLEAN";
                        }
                    }
                    else if (column.Type == 'M')
                    {
                        pgColumnType = "TEXT";
                    }
                    else
                    {
                        pgColumnType = $"VARCHAR({column.Length})";
                    }
                    if (column.Export)
                    {
                        columnNames.Add(columnName + " " + pgColumnType);
                    }
                }
                else if (column.Export)
                {
                    columnNames.Add(columnName);
                }
            }
            InitMemoFile();
            DbfFile.Seek(SkipBytes + 1, SeekOrigin.Current);
            return columnNames;
        }

        private void InitMemoFile()
        {
            var memoColumns = Columns.Where(c => c.Export && c.Type == 'M');
            if (memoColumns.Any())
            {
                string memoExtension;
                int length = memoColumns.First().Length;
                if (length == 4)
                {
                    memoExtension = ".fpt";
                }
                else if (length == 10)
                {
                    memoExtension = ".dbt";
                    MemoBlockSize = 0x200; //standard block size for dbt files
                }
                else
                {
                    throw new Exception("Invalid memo field");
                }
                string memoFileName = Path.Combine(Path.GetDirectoryName(DbfFileName),Path.GetFileNameWithoutExtension(dbfFileName)+memoExtension);
                if (File.Exists(memoFileName))
                {
                    MemoFile = new FileStream(memoFileName, FileMode.Open, FileAccess.Read);
                    if (memoExtension == ".fpt")
                    {
                        var blockSize = new byte[2];
                        MemoFile.Seek(6, SeekOrigin.Begin);
                        MemoFile.Read(blockSize, 0, 2);
                        MemoBlockSize = blockSize[0] << 8 | blockSize[1];
                    }
                    foreach(var column in memoColumns)
                    {
                        var memoColumn = column as MemoColumn;
                        memoColumn.MemoBlockSize = MemoBlockSize;
                        memoColumn.MemoFile = MemoFile;
                    }
                }
                else
                {
                    throw new FileNotFoundException("Memo file not found");
                }
            }
        }
    }
}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       