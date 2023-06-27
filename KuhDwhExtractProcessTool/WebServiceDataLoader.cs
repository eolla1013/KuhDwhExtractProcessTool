using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http;
using System.Runtime.Serialization;
using System.Text;
using System.Xml;
using System.Threading.Tasks;

namespace KuhDwhExtractProcessTool
{
    class WebServiceDataLoader
    {
        private NLog.ILogger Logger;
        private ExtractPhase Config;

        public WebServiceDataLoader(ExtractPhase phase) {
            this.Logger = NLog.LogManager.GetLogger(this.GetType().ToString());
            this.Config = phase;
        }

        public void LoadAndJoin() {
            this.Logger.Info("-- Load Join Data: {0} base {1}", this.Config.OutputName, this.Config.SourceName);

            DataTable outtbl = new DataTable(this.Config.OutputName);
            DataTable srctbl = this.Config.GetSourceDataTable();

            //出力テーブル定義
            foreach (var coldef in this.Config.JoinColumnList) {
                DataTable deftbl;
                if (coldef.SourceTableName == "@Loaded") {
                    outtbl.Columns.Add(coldef.DestinationColumnName, coldef.GetDestinationColumnType());
                } else {
                    if (coldef.SourceTableName == "@Source") {
                        deftbl = srctbl;
                    } else {
                        deftbl = this.Config.GetDataTable(coldef.SourceTableName);
                    }
                    if (coldef.IsDefinedType) {
                        outtbl.Columns.Add(coldef.DestinationColumnName, coldef.GetDestinationColumnType());
                    } else {
                        var col = deftbl.Columns[coldef.SourceColumnName];
                        outtbl.Columns.Add(coldef.DestinationColumnName, col.DataType);
                    }
                }
            }

            //WebServiceでデータ取得
            foreach (DataRow srcrow in srctbl.Rows) {
                var param = new SortedList<string, string>();
                foreach (var prm in this.Config.WebServiceParameterList) {
                    param.Add(prm.Name, (string)prm.GetValueFromDataRow(srcrow));
                }
                var doc = this.GetWebApi(this.Config.WebServiceUrl, param);

                
                //条件フィルタ
                XmlNode ldnd=null;
                if (doc != null) {
                    if (string.IsNullOrWhiteSpace(this.Config.WebServiceFilterExpression)) {
                        ldnd = doc.FirstChild;
                    } else {
                        var rows = doc.SelectNodes(this.Config.WebServiceFilterExpression);
                        if (rows.Count > 0) {
                            ldnd = rows[0];
                        }
                    }
                }

                //出力テーブルに出力
                var outrow = outtbl.NewRow();
                foreach (var coldef in this.Config.JoinColumnList) {
                    if (coldef.SourceTableName == "@Source") {
                        if (coldef.IsDefinedType) {
                            outrow[coldef.DestinationColumnName] = coldef.ConvertDestinationValue(srcrow[coldef.SourceColumnName].ToString());
                        } else {
                            outrow[coldef.DestinationColumnName] = srcrow[coldef.SourceColumnName];
                        }
                    } else if (coldef.SourceTableName == "@Loaded") {
                        if (ldnd != null) {
                            var elem = ldnd.SelectSingleNode(coldef.SourceColumnName);
                            if (elem != null) {
                                var strval = elem.InnerText;
                                if (coldef.IsDefinedType) {
                                    outrow[coldef.DestinationColumnName] = coldef.ConvertDestinationValue(strval);
                                } else {
                                    outrow[coldef.DestinationColumnName] = strval;
                                }
                            }
                        }
                    } else {
                        //特定のテーブルのデータなので検索処理が必要
                    }
                }
                outtbl.Rows.Add(outrow);
            }

            this.Config.SetDataTable(outtbl);
        }

        private XmlDocument GetWebApi(string url,SortedList<string,string> param) {
            XmlDocument retdoc = null;
            using (var cl = new HttpClient()) {
                var srvurl = new StringBuilder();
                srvurl.Append(url);
                foreach (var itm in param) {
                    srvurl.Replace("@" + itm.Key, itm.Value);
                }

                HttpResponseMessage res = cl.GetAsync(srvurl.ToString()).Result;

                if (res.IsSuccessStatusCode) {

                    using (var resstream = res.Content.ReadAsStreamAsync().Result) {
                        var reader = System.Runtime.Serialization.Json.JsonReaderWriterFactory.CreateJsonReader(resstream,XmlDictionaryReaderQuotas.Max);
                        retdoc = new XmlDocument();
                        retdoc.Load(reader);
                    }
                } else if (res.StatusCode == System.Net.HttpStatusCode.NotFound) {
                    //NULLを返す
                } else {
                    this.Logger.Error("WebServiceDataLoader.GetWebApi:{0}", res.ToString());
                    this.Logger.Error("Request Message:{0}", res.RequestMessage.ToString());
                }

            }
            return retdoc;
        }

    }
}
