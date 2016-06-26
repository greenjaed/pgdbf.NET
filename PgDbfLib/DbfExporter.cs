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

        private long recordCount;
        private int skipBytes;
        private int fieldArraySize;
        private int fieldCount;
        private FileStream dbfFile;
        private List<DbfColumn> columns;
        private static int headerSize = 32;
        private static int fieldDefinitionSize = 32;
        private static string[] reservedWords = new string[]
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
        
        private class DbfColumn
        {
            public bool Export { get; set; }
            public int Length { get; set; }
            public char Type
            {
                get
                {
                    return type;
                }
                set
                {
                    type = value;
                    if (value == 'L')
                    {
                        Normalize = s => s == "Y" || s == "T" ? "t" : "f";
                    }
                }
            }
            private char type;
            public Func<string, string> Normalize { get; private set; }
        }

        public DbfExporter()
        {
            truncateTable = false;
            createTable = true;
            dropTable = true;
            ConvertNumericToText = false;
            WrapInTransaction = true;
            PGSqlVersion = PgVersion.Pg8_2_And_Newer;
            IncludedColumns = new List<string>();
            ColumnRenames = new Dictionary<string, string>();
            columns = new List<DbfColumn>();
        }

        public DbfExporter(string dbfFileName) : this()
        {
            DbfFileName = dbfFileName;
        }

        /// <summary>
        /// Creates a PostGresql dump of the indicated dbf file.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetPgScript()
        {
            if (!File.Exists(DbfFileName))
            {
                throw new Exception("The Dbf File does not exist or is inaccessible");
            }
            ColumnRenames = ColumnRenames.ToDictionary(d => d.Key.ToUpper(), d => d.Value);
            IncludedColumns = IncludedColumns.Select(s => s.ToUpper()).ToList();
            dbfFile = new FileStream(dbfFileName, FileMode.Open, FileAccess.Read);
            readHeader();
            if (WrapInTransaction)
            {
                yield return "BEGIN;";
            }
            if (DropTable)
            {
                yield return string.Format("SET statement_timeout = 60000; DROP TABLE {0}{1};  SET statement_timeout=0;",
                    PGSqlVersion == PgVersion.Pg8_2_And_Newer ? "IF EXISTS " : string.Empty, TableName);
            }
            string columnString = getColumns();
            yield return CreateTable ? string.Format("CREATE TABLE {0} ({1});", TableName, columnString) :
                columnString;
            if (TruncateTable)
            {
                yield return string.Format("TRUNCATE TABLE {0};", TableName);
            }
            yield return string.Format(@"COPY {0} FROM STDIN", TableName);
            dbfFile.Seek(1, SeekOrigin.Current);
            foreach (string row in getRows())
            {
                yield return row;
            }
            yield return @"\.";
            if (WrapInTransaction)
            {
                yield return "COMMIT;";
            }
        }

        private void readHeader()
        {
            byte[] header = new byte[headerSize];
            dbfFile.Read(header, 0, headerSize);
            if (header[0] == 0x30)
            {
                skipBytes = 263;
            }
            recordCount = BitConverter.ToUInt32(header, 4);
            int headerLength = BitConverter.ToUInt16(header, 8);
            fieldArraySize = headerLength - headerSize - skipBytes - 1;
            if (fieldArraySize % fieldDefinitionSize == 1)
            {
                ++skipBytes;
                --fieldArraySize;
            }
            fieldCount = fieldArraySize / fieldDefinitionSize;
            dbfFile.Seek(skipBytes, SeekOrigin.Current);
        }

        private IEnumerable<string> getRows()
        {
            List<string> row;
            int rowLength = columns.Sum(c => c.Length);
            char[] rowField = new char[columns.Max(c => c.Length)];
            string fieldValue;
            using (StreamReader rowReader = new StreamReader(dbfFile))
            {
                for (int i = 0; i < recordCount; ++i)
                {
                    if ((char)rowReader.Read() == '*')
                    {
                        char[] deleted = new char[rowLength];
                        rowReader.Read(deleted, 0, rowLength);
                        continue;
                    }
                    row = new List<string>();
                    foreach (var field in columns)
                    {
                        rowReader.Read(rowField, 0, field.Length);
                        if (field.Export)
                        {
                            fieldValue = new string(rowField, 0, field.Length).Replace("\0", string.Empty);
                            if (field.Normalize != null)
                            {
                                row.Add(field.Normalize(fieldValue));
                            }
                            else
                            {
                                row.Add(fieldValue);
                            }
                        }
                    }
                    yield return string.Join("\t", row);
                }
            }
        }

        private string getColumns()
        {
            bool addColumn = IncludedColumns.Count == 0;
            List<string> columnNames = new List<string>();
            byte[] columnHeader = new byte[fieldArraySize];
            dbfFile.Read(columnHeader, 0, fieldArraySize);
            int baseOffset;
            int numericDecimal;
            string columnType;
            for (int i = 0; i < fieldCount; ++i)
            {
                baseOffset = i*fieldDefinitionSize;
                var column = new DbfColumn();
                byte[] nameBytes = new byte[11];
                Array.Copy(columnHeader, baseOffset, nameBytes, 0, 11);
                string columnName = Encoding.ASCII.GetString(nameBytes).Replace("\0", string.Empty);
                column.Export = IncludedColumns.Contains(columnName) || addColumn;
                if (ColumnRenames.ContainsKey(columnName))
                {
                    columnName = ColumnRenames[columnName];
                }
                if (reservedWords.Contains(columnName))
                {
                    int increment = 0;
                    while (IncludedColumns.Contains(string.Format("{0}_{1}", columnName, ++increment)));
                    columnName = string.Format("{0}_{1}", columnName, increment);
                }
                if (addColumn)
                {
                    IncludedColumns.Add(columnName);
                }
                column.Type = Convert.ToChar(columnHeader[baseOffset + 11]);
                column.Length = columnHeader[baseOffset + 16];
                columns.Add(column);
                if (CreateTable)
                {
                    if (column.Type == 'N')
                    {
                        if (ConvertNumericToText)
                        {
                            columnType = "TEXT";
                        }
                        else
                        {
                            numericDecimal = columnHeader[baseOffset + 17];
                            if (numericDecimal == 0)
                            {
                                columnType = string.Format("NUMERIC({0})", column.Length);
                            }
                            else
                            {
                                columnType = string.Format("NUMERIC({0},{1})", column.Length, numericDecimal);
                            }
                        }
                    }
                    else if (column.Type == 'L')
                    {
                        columnType = "BOOLEAN";
                    }
                    else
                    {
                        columnType = string.Format("VARCHAR({0})", column.Length);
                    }
                    columnNames.Add(columnName + " " + columnType);
                }
                else
                {
                    columnNames.Add(columnName);
                }
            }
            return string.Join(",", columnNames);
        }
    }
}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       