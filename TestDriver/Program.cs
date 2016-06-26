using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PgDbfLib;

namespace TestDriver
{
    class Program
    {
        static void Main(string[] args)
        {
            DbfExporter exporter = new DbfExporter();
            exporter.DbfFileName = "test.dbf";
            foreach (var line in exporter.GetPgScript())
            {
                Console.WriteLine(line);
            }
        }
    }
}
