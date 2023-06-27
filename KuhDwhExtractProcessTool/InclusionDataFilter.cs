using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KuhDwhExtractProcessTool
{
    class InclusionDataFilter
    {
        private NLog.ILogger Logger;
        private ExtractPhase Config;

        public InclusionDataFilter(ExtractPhase phase) {
            this.Logger = NLog.LogManager.GetLogger(this.GetType().ToString());
            this.Config = phase;
        }

        public void Filter() {
            this.Logger.Info("-- Filter Data: {0} from {1}", this.Config.OutputName, this.Config.SourceName);
            var srctbl = this.Config.GetSourceDataTable();
            var outtbl = srctbl.Clone();
            outtbl.TableName = this.Config.OutputName;

            var rows = srctbl.Select(this.Config.FilterExpression);
            foreach (DataRow row in rows) {
                outtbl.ImportRow(row);
            }

            this.Config.SetDataTable(outtbl);
        }

    }
}
