using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace KuhCommonLib
{
    public class CsvFileWriter
    {
        public DataTable CsvData { get; set; }

        private System.Text.Encoding FileEncoding;

        public CsvFileWriter() {
            this.FileEncoding = System.Text.Encoding.GetEncoding("Shift_JIS");
        }

        public CsvFileWriter(string encname) {
            this.FileEncoding = System.Text.Encoding.GetEncoding(encname);
        }

        public void Write(string outfilename) {
            using (System.IO.StreamWriter writer = new System.IO.StreamWriter(outfilename, false, this.FileEncoding)) {
                StringBuilder line = new StringBuilder();
                foreach (DataColumn col in this.CsvData.Columns) {
                    if (line.Length > 0) {
                        line.Append(",");
                    }
                    line.Append("\"");
                    line.Append(col.ColumnName);
                    line.Append("\"");
                }
                writer.WriteLine(line.ToString());
                foreach (DataRow row in this.CsvData.Rows) {
                    line = new StringBuilder();
                    foreach (var itm in row.ItemArray) {
                        if (line.Length > 0) {
                            line.Append(",");
                        }
                        line.Append("\"");
                        line.Append(this.GetCsvValue(itm));
                        line.Append("\"");
                    }
                    writer.WriteLine(line.ToString());
                }
            }
        }

        private string GetCsvValue(object inval) {
            if (inval == null) {
                return "";
            }
            var str = inval.ToString();
            str = str.Replace("\"", "\"\"");

            return str;
        }

    }
}
