using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KuhCommonLib;

namespace KuhDwhExtractProcessTool
{
    class TableDataConverter
    {
        private NLog.ILogger Logger;
        private ExtractPhase Config;

        public TableDataConverter(ExtractPhase phase) {
            this.Logger = NLog.LogManager.GetLogger(this.GetType().ToString());
            this.Config = phase;
        }

        public void Convert() {
            this.Logger.Info("-- Convert Table: {0} to {1}", this.Config.SourceName, this.Config.OutputName);

            DataTable srctbl = this.Config.GetSourceDataTable();
            DataTable outtbl = new DataTable(this.Config.OutputName);
            foreach (var coldef in this.Config.ConvertColumnList) {
                if (coldef.ConvertType == "Move") {
                    if (!srctbl.Columns.Contains(coldef.SourceColumnName)) {
                        throw new ApplicationException("ソースカラムに存在しない列が指定されました。:" + coldef.SourceColumnName);
                    }
                    var col = srctbl.Columns[coldef.SourceColumnName];
                    outtbl.Columns.Add(coldef.DestinationColumnName, col.DataType);
                } else if (coldef.ConvertType == "Format") {
                    outtbl.Columns.Add(coldef.DestinationColumnName, coldef.GetDestinationColumnType());
                } else {
                    outtbl.Columns.Add(coldef.DestinationColumnName, coldef.GetDestinationColumnType());
                }
            }
            foreach (DataRow srcrow in srctbl.Rows) {
                var outrow = outtbl.NewRow();
                foreach (var coldef in this.Config.ConvertColumnList) {
                    if (coldef.ConvertType == "Move") {
                        outrow[coldef.DestinationColumnName] = srcrow[coldef.SourceColumnName];
                    } else if (coldef.ConvertType == "Format") {
                        outrow[coldef.DestinationColumnName] = coldef.GetFormatValueFromDataRow(srcrow);
                    } else {
                        outrow[coldef.DestinationColumnName] = coldef.GetFunctionValueFromDataRow(srcrow);
                    }
                }
                outtbl.Rows.Add(outrow);
            }

            this.Config.SetDataTable(outtbl);
        }
    }
}
