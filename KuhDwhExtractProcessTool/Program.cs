using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KuhDwhExtractProcessTool
{
    class Program
    {
        static void Main(string[] args) {

            //ExtractCIDP();
            //WebApiTest();

            if (args.Count() > 0) {
                var process = new ExtractProcess(args[0]);
                for(int i = 1; i < args.Count(); i++) {
                    process.SetParameter("ARGS" + i, args[i]);
                }
                process.Run();
            } else {
                Console.WriteLine("引数エラー！設定ファイル名を指定してください。");
            }

        }

        private static void ExtractCIDP() {
            var process = new ExtractProcess("CIDP患者来院履歴抽出.json");

            var phase = new ExtractPhase();
            phase.ActionType = "LoadFromODBC";
            phase.ConnectionName = "KuhDwh";
            phase.SqlText = @"
select distinct dis.ptid from m3_data_table_kuh_his.TBYOMEI dis
where dis.byomeicd='8841670' and dis.startdate<=? and (dis.tenkidate>=? or dis.tenkidate='1840-12-31') and dis.utagaikbn='0' and dis.sbyomeikbn='1'
";
            phase.SqlParameterList.Add(new ExtractPhase.SqlParameter() {
                Name="EDDT",DataType="Date",Value="2019-06-30"
            });
            phase.SqlParameterList.Add(new ExtractPhase.SqlParameter() {
                Name = "STDT",
                DataType = "Date",
                Value = "2019-06-01"
            });
            phase.OutputName = "CIDPPATIENT";
            process.AddPhase(phase);

            phase = new ExtractPhase();
            phase.ActionType = "WriteToCsvFile";
            phase.SourceName = "CIDPPATIENT";
            phase.FileName = "CIDP患者ID.csv";
            process.AddPhase(phase);

            phase = new ExtractPhase();
            phase.ActionType = "LoadJoinFromODBC";
            phase.ConnectionName = "KuhDwh";
            phase.SourceName = "CIDPPATIENT";
            phase.JoinType = "First";
            phase.SqlText = @"
select n.ptid,n.nyuindate,n.nyuintime,n.taiindate,n.taiintime,n.nyuinstatus
FROM m3_data_table_kuh_his.TNYUIN N
where n.ptid=? and n.nyuinstatus in ('1','2') and n.nyuindate<=? and (n.taiindate>=? or n.taiindate='1840-12-31')
order by n.nyuindate,n.nyuintime
";
            phase.SqlParameterList.Add(new ExtractPhase.SqlParameter() {
                Name = "PTID",
                DataType = "String",
                Value = "@PTID"
            });
            phase.SqlParameterList.Add(new ExtractPhase.SqlParameter() {
                Name = "EDDT",
                DataType = "Date",
                Value = "2019-06-30"
            });
            phase.SqlParameterList.Add(new ExtractPhase.SqlParameter() {
                Name = "STDT",
                DataType = "Date",
                Value = "2019-06-01"
            });
            phase.OutputName = "CIDPDATA1";
            phase.JoinColumnList.Add(new ExtractPhase.JoinColumn() {
                SourceTableName= "@Source",
                SourceColumnName ="PTID",
                DestinationColumnName ="PTID"
            });
            phase.JoinColumnList.Add(new ExtractPhase.JoinColumn() {
                SourceTableName = "@Loaded",
                SourceColumnName = "NYUINDATE",
                DestinationColumnName = "NYUINDATE"
            });
            phase.JoinColumnList.Add(new ExtractPhase.JoinColumn() {
                SourceTableName = "@Loaded",
                SourceColumnName = "TAIINDATE",
                DestinationColumnName = "TAIINDATE"
            });
            phase.JoinColumnList.Add(new ExtractPhase.JoinColumn() {
                SourceTableName = "@Loaded",
                SourceColumnName = "NYUINSTATUS",
                DestinationColumnName = "NYUINSTATUS"
            });
            process.AddPhase(phase);

            phase = new ExtractPhase();
            phase.ActionType = "LoadJoinFromODBC";
            phase.ConnectionName = "KuhDwh";
            phase.SourceName = "CIDPDATA1";
            phase.JoinType = "First";
            phase.SqlText = @"
select app.ykptid,app.ykdate,app.yktime,app.ykkacd,app.ykdrid,app.ykname
from m3_data_table_kuh_his.TYOYAKUR app
where app.ykptid=? and app.ykdate>=? and app.ykdate<=? and app.ykodkind='51' and (app.ykcancel<>'1' or app.ykcancel is null) and (app.yksincd<>'5' or app.yksincd is null)
order by app.ykdate,app.yktime
";
            phase.SqlParameterList.Add(new ExtractPhase.SqlParameter() {
                Name = "PTID",
                DataType = "String",
                Value = "@PTID"
            });
            phase.SqlParameterList.Add(new ExtractPhase.SqlParameter() {
                Name = "STDT",
                DataType = "Date",
                Value = "2019-06-01"
            });
            phase.SqlParameterList.Add(new ExtractPhase.SqlParameter() {
                Name = "EDDT",
                DataType = "Date",
                Value = "2019-06-30"
            });
            phase.OutputName = "CIDPDATA2";
            phase.JoinColumnList.Add(new ExtractPhase.JoinColumn() {
                SourceTableName = "@Source",
                SourceColumnName = "PTID",
                DestinationColumnName = "PTID"
            });
            phase.JoinColumnList.Add(new ExtractPhase.JoinColumn() {
                SourceTableName = "@Source",
                SourceColumnName = "NYUINDATE",
                DestinationColumnName = "NYUINDATE"
            });
            phase.JoinColumnList.Add(new ExtractPhase.JoinColumn() {
                SourceTableName = "@Source",
                SourceColumnName = "TAIINDATE",
                DestinationColumnName = "TAIINDATE"
            });
            phase.JoinColumnList.Add(new ExtractPhase.JoinColumn() {
                SourceTableName = "@Source",
                SourceColumnName = "NYUINSTATUS",
                DestinationColumnName = "NYUINSTATUS"
            });
            phase.JoinColumnList.Add(new ExtractPhase.JoinColumn() {
                SourceTableName = "@Loaded",
                SourceColumnName = "YKDATE",
                DestinationColumnName = "YKDATE"
            });
            phase.JoinColumnList.Add(new ExtractPhase.JoinColumn() {
                SourceTableName = "@Loaded",
                SourceColumnName = "YKKACD",
                DestinationColumnName = "YKKACD"
            });
            phase.JoinColumnList.Add(new ExtractPhase.JoinColumn() {
                SourceTableName = "@Loaded",
                SourceColumnName = "YKDRID",
                DestinationColumnName = "YKDRID"
            });
            process.AddPhase(phase);

            phase = new ExtractPhase();
            phase.ActionType = "FilterInclusion";
            phase.SourceName = "CIDPDATA2";
            phase.FilterExpression = "NYUINDATE IS NOT NULL OR YKDATE IS NOT NULL";
            phase.OutputName = "CIDPOUTDATA";
            process.AddPhase(phase);

            phase = new ExtractPhase();
            phase.ActionType = "WriteToCsvFile";
            phase.SourceName = "CIDPOUTDATA";
            phase.FileName = "CIDP患者データ.csv";
            process.AddPhase(phase);

            process.Run();
            process.WriteConfig("CIDP患者来院履歴抽出.json");
        }

        private static void WebApiTest() {

            var loader = new WebServiceDataLoader(new ExtractPhase());
            loader.TestWebService();

        }
    }
}
