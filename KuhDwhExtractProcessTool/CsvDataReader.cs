using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KuhDwhExtractProcessTool
{
    class CsvDataReader
    {
        private NLog.ILogger Logger;
        private ExtractPhase Config;
        public CsvDataReader(ExtractPhase phase) {
            this.Logger = NLog.LogManager.GetLogger(this.GetType().ToString());
            this.Config = phase;
        }

        public void Load() {
            this.Logger.Info("-- Load Data: {0}", this.Config.OutputName);
            
            DataTable tbl = this.LoadCsv(this.GetFileName(), this.Config.OutputName);
            
            this.Config.SetDataTable(tbl);
        }

        public void LoadAndJoin() {
            this.Logger.Info("-- Load Join Data: {0} base {1}", this.Config.OutputName, this.Config.SourceName);

            DataTable outtbl = new DataTable(this.Config.OutputName);
            DataTable ldtbl = this.LoadCsv(this.GetFileName(), "LoadedTable");
            DataTable srctbl = this.Config.GetSourceDataTable();

            //出力テーブル定義設定
            foreach (var coldef in this.Config.JoinColumnList) {
                DataTable deftbl;
                if (coldef.SourceTableName == "@Source") {
                    deftbl = srctbl;
                } else if (coldef.SourceTableName == "@Loaded") {
                    deftbl = ldtbl;
                } else {
                    deftbl = this.Config.GetDataTable(coldef.SourceTableName);
                }
                if (coldef.IsDefinedType) {
                    outtbl.Columns.Add(coldef.DestinationColumnName, coldef.GetDestinationColumnType());
                } else {
                    if (!deftbl.Columns.Contains(coldef.SourceColumnName)) {
                        throw new ApplicationException("ソースカラムに存在しない列が指定されました。:" + coldef.SourceColumnName);
                    }
                    var col = deftbl.Columns[coldef.SourceColumnName];
                    outtbl.Columns.Add(coldef.DestinationColumnName, col.DataType);
                }
            }

            //データ結合
            foreach (DataRow srcrow in srctbl.Rows) {
                var selstr = new StringBuilder();
                selstr.Append(this.Config.FilterExpression);
                foreach (var prm in this.Config.FilterParameterList) {
                    var val = prm.GetValueFromDataRow(srcrow).ToString();
                    if (prm.IsQuotationMark) {
                        val = "'" + val + "'";
                    }
                    selstr.Replace("@" + prm.Name, val);
                }
                var sortstr = this.Config.SortExpression;

                var ldrows = ldtbl.Select(selstr.ToString(), sortstr);
                if (ldrows.Count() > 0) {
                    bool firstflg = true;
                    foreach (DataRow ldrow in ldrows) {
                        if (this.Config.JoinType == "First" && !firstflg) break;
                        firstflg = false;
                        var outrow = outtbl.NewRow();
                        foreach (var coldef in this.Config.JoinColumnList) {
                            if (coldef.SourceTableName == "@Source") {
                                if (coldef.IsDefinedType) {
                                    outrow[coldef.DestinationColumnName] = coldef.ConvertDestinationValue(srcrow[coldef.SourceColumnName].ToString());
                                } else {
                                    outrow[coldef.DestinationColumnName] = srcrow[coldef.SourceColumnName];
                                }
                            } else if (coldef.SourceTableName == "@Loaded") {
                                if (coldef.IsDefinedType) {
                                    outrow[coldef.DestinationColumnName] = coldef.ConvertDestinationValue(ldrow[coldef.SourceColumnName].ToString());
                                } else {
                                    outrow[coldef.DestinationColumnName] = ldrow[coldef.SourceColumnName];
                                }
                            } else {
                                //特定のテーブルのデータなので検索処理が必要
                            }
                        }
                        outtbl.Rows.Add(outrow);
                    }
                } else {
                    var outrow = outtbl.NewRow();
                    foreach (var coldef in this.Config.JoinColumnList) {
                        if (coldef.SourceTableName == "@Source") {
                            if (coldef.IsDefinedType) {
                                outrow[coldef.DestinationColumnName] = coldef.ConvertDestinationValue(srcrow[coldef.SourceColumnName].ToString());
                            } else {
                                outrow[coldef.DestinationColumnName] = srcrow[coldef.SourceColumnName];
                            }
                            
                        } else if (coldef.SourceTableName == "@Loaded") {
                            //1件もデータがないのでスキップ
                        } else {
                            //特定のテーブルのデータなので検索処理が必要
                        }
                    }
                    outtbl.Rows.Add(outrow);
                }
            }

            this.Config.SetDataTable(outtbl);
        }

        public void LoadAndMerge() {
            this.Logger.Info("-- Load Join Data: {0} base {1}", this.Config.OutputName, this.Config.SourceName);

            DataTable outtbl = new DataTable(this.Config.OutputName);
            DataTable ldtbl = this.LoadCsv(this.Config.FileName, "LoadedTable");
            DataTable srctbl = this.Config.GetSourceDataTable();

            //出力テーブル定義設定
            foreach (var coldef in this.Config.JoinColumnList) {
                DataTable deftbl;
                if (coldef.SourceTableName == "@Source") {
                    deftbl = srctbl;
                } else if (coldef.SourceTableName == "@Loaded") {
                    deftbl = ldtbl;
                } else {
                    deftbl = this.Config.GetDataTable(coldef.SourceTableName);
                }
                if (coldef.IsDefinedType) {
                    outtbl.Columns.Add(coldef.DestinationColumnName, coldef.GetDestinationColumnType());
                } else {
                    if (!deftbl.Columns.Contains(coldef.SourceColumnName)) {
                        throw new ApplicationException("ソースカラムに存在しない列が指定されました。:" + coldef.SourceColumnName);
                    }
                    var col = deftbl.Columns[coldef.SourceColumnName];
                    outtbl.Columns.Add(coldef.DestinationColumnName, col.DataType);
                }
            }
            //ソーステーブル出力
            foreach (DataRow srcrow in srctbl.Rows) {
                var outrow = outtbl.NewRow();
                foreach (var coldef in this.Config.JoinColumnList) {
                    if (srctbl.Columns.Contains(coldef.SourceColumnName)) {
                        if (coldef.IsDefinedType) {
                            outrow[coldef.DestinationColumnName] = coldef.ConvertDestinationValue(srcrow[coldef.SourceColumnName].ToString());
                        } else {
                            outrow[coldef.DestinationColumnName] = srcrow[coldef.SourceColumnName];
                        }
                    }
                }
                outtbl.Rows.Add(outrow);
            }
            //ロードテーブルマージ
            outtbl.DefaultView.Sort = this.Config.SortExpression;
            foreach (DataRow ldrow in ldtbl.Rows) {
                var selstr = new StringBuilder();
                selstr.Append(this.Config.FilterExpression);
                foreach (var prm in this.Config.FilterParameterList) {
                    var val = prm.GetValueFromDataRow(ldrow).ToString();
                    if (prm.IsQuotationMark) {
                        val = "'" + val + "'";
                    }
                    selstr.Replace("@" + prm.Name, val);
                }
                var sortstr = this.Config.SortExpression;

                var outrows = outtbl.Select(selstr.ToString(), sortstr);
                if (outrows.Count() > 0) {
                    var outrow = outrows.First();
                    foreach (var coldef in this.Config.JoinColumnList) {
                        if (coldef.SourceTableName == "@Loaded") {
                            if (coldef.IsDefinedType) {
                                outrow[coldef.DestinationColumnName] = coldef.ConvertDestinationValue(ldrow[coldef.SourceColumnName].ToString());
                            } else {
                                outrow[coldef.DestinationColumnName] = ldrow[coldef.SourceColumnName];
                            }
                        }
                    }
                } else {
                    var outrow = outtbl.NewRow();
                    foreach (var coldef in this.Config.JoinColumnList) {
                        if (ldtbl.Columns.Contains(coldef.SourceColumnName)) {
                            if (coldef.IsDefinedType) {
                                outrow[coldef.DestinationColumnName] = coldef.ConvertDestinationValue(ldrow[coldef.SourceColumnName].ToString());
                            } else {
                                outrow[coldef.DestinationColumnName] = ldrow[coldef.SourceColumnName];
                            }
                        }
                    }
                    outtbl.Rows.Add(outrow);
                }

            }

            this.Config.SetDataTable(outtbl);
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

        private DataTable LoadCsv(string filename,string tblname) {

            DataTable tbl = new DataTable(tblname);
            KuhCommonLib.CsvFileReader reader;
            if (this.Config.IsSelectEncoding()) {
                reader = new KuhCommonLib.CsvFileReader(this.Config.Encoding);
            } else {
                reader = new KuhCommonLib.CsvFileReader();
            }
            
            reader.HeaderExists = true;
            reader.Read(filename);
            tbl.Merge(reader.CsvData);

            return tbl;
        }

    }
}
