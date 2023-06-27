using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KuhDwhExtractProcessTool
{
    class FlattenTable
    {
        private NLog.ILogger Logger;
        private ExtractPhase Config;

        public FlattenTable(ExtractPhase phase) {
            this.Logger = NLog.LogManager.GetLogger(this.GetType().ToString());
            this.Config = phase;
        }

        public void Flatten() {
            this.Logger.Info("-- Flatten Table Data: {0} base {1}", this.Config.OutputName, this.Config.SourceName);

            DataTable srctbl = this.Config.GetSourceDataTable();

            DataTable outtbl = new DataTable(this.Config.OutputName);
            this.Config.KeyColumnList.ForEach(i => {
                outtbl.Columns.Add(srctbl.Columns[i].ColumnName, srctbl.Columns[i].DataType);
            });
            this.Config.FlattenColumnList.ForEach(i => {
                var col = srctbl.Columns[i.ValueColumnName];
                outtbl.Columns.Add(i.DestinationColumnName, col.DataType);
            });

            var srcrows=srctbl.Select("", this.Config.GetKeyColumnNameCsvString());
            string prekey = "";
            DataRow outrow = null;
            foreach (DataRow srcrow in srcrows) {
                var key = this.GetKeyData(srcrow);
                if (key != prekey) {
                    outrow = outtbl.NewRow();
                    this.Config.KeyColumnList.ForEach(i => {
                        outrow[i] = srcrow[i];
                    });
                    outtbl.Rows.Add(outrow);
                    prekey = key;
                }
                this.Config.FlattenColumnList.ForEach(flatcol => {
                    var keyval = srcrow[flatcol.KeyColumnName].ToString();
                    if (keyval == flatcol.KeyColumnValue) {
                        if (outrow.IsNull(flatcol.DestinationColumnName)) {
                            outrow[flatcol.DestinationColumnName] = srcrow[flatcol.ValueColumnName];
                        } else {
                            //すでに入っていたら文字列とみなして連結
                            var tmpval = outrow[flatcol.DestinationColumnName].ToString();
                            tmpval = tmpval + "," + srcrow[flatcol.ValueColumnName].ToString();
                            outrow[flatcol.DestinationColumnName] = tmpval;
                        }
                    }
                });
            }

            this.Config.SetDataTable(outtbl);
        }

        private string GetKeyData(DataRow row) {
            var str = new StringBuilder();
            this.Config.KeyColumnList.ForEach(i => {
                if (str.Length > 0) {
                    str.Append("|");
                }
                str.Append(row[i].ToString());
            });

            return str.ToString();
        }

    }
}
