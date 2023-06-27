using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KuhDwhExtractProcessTool
{
    class CsvDataWriter
    {
        private NLog.ILogger Logger;
        private ExtractPhase Config;
        public CsvDataWriter(ExtractPhase phase) {
            this.Logger = NLog.LogManager.GetLogger(this.GetType().ToString());
            this.Config = phase;
        }

        public void Write() {
            var filename = this.GetFileName();
            var tbl= this.Config.GetSourceDataTable();
            this.Logger.Info("-- Write CSV: Source={0}, File={1}", this.Config.SourceName, filename);

            if (tbl.Rows.Count == 0 && this.Config.NoDataOutputFile!=true) {
                return;
            }

            KuhCommonLib.CsvFileWriter writer;
            if (this.Config.IsSelectEncoding()) {
                writer = new KuhCommonLib.CsvFileWriter(this.Config.Encoding);
            } else {
                writer = new KuhCommonLib.CsvFileWriter();
            }
            writer.CsvData = tbl;
            writer.Write(filename);
        }

        private string GetFileName() {
            if (this.Config.FileName.Substring(0, 1) == "@") {
                var param = this.Config.FileName.Substring(1);

                if (this.Config.ContainsParameter(param)) {
                    return this.Config.GetParameter(param);
                } else {
                    throw new ArgumentException("存在しないパラメータが設定されている:" + param);
                }
            } else {
                return this.Config.FileName;
            }
        }
    }
}
