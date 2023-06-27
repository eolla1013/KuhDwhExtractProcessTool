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
    class OdbcDataLoader
    {
        private NLog.ILogger Logger;
        private ExtractPhase Config;

        public OdbcDataLoader(ExtractPhase phase) {
            this.Logger = NLog.LogManager.GetLogger(this.GetType().ToString());
            this.Config = phase;
        }

        public void Load() {
            this.Logger.Info("-- Load Data: {0}", this.Config.OutputName);

            DataTable tbl = new DataTable(this.Config.OutputName);
            using(var conn = this.GetConnection()) {
                conn.Open();
                using(var cmd = conn.CreateCommand()) {
                    var sql = this.Config.GetSqlText();
                    foreach (var param in this.Config.SqlParameterList) {
                        object val = null;
                        if (param.IsVariable) {
                            val = this.Config.GetParameter(param.VariableName);
                        } else {
                            val = param.Value;
                        }
                        if (param.IsPlaceHolderParameter) {
                            cmd.Parameters.Add(param.Name, this.GetDataType(param.DataType)).Value = val;
                        }
                        if (param.IsReplaceParameter) {
                            sql = sql.Replace("{" + param.Name + "}", (string)val);
                        }
                    }
                    cmd.CommandText = sql;
                    cmd.CommandTimeout = 0;
                    //this.Logger.DebugFormat("SQL:{0}", cmd.CommandText);
                    using (var adapter = new OdbcDataAdapter()) {
                        adapter.SelectCommand = cmd;
                        adapter.Fill(tbl);
                    }
                }
                conn.Close();
            }
            this.Config.SetDataTable(tbl);
        }

        public void LoadAndJoin() {
            this.Logger.Info("-- Load Join Data: {0} base {1}", this.Config.OutputName,this.Config.SourceName);

            DataTable outtbl = new DataTable(this.Config.OutputName);
            DataTable srctbl = this.Config.GetSourceDataTable();
            using (var conn = this.GetConnection()) {
                conn.Open();
                foreach (DataRow srcrow in srctbl.Rows) {
                    //結合テーブル読み込み
                    DataTable ldtbl = new DataTable();
                    using (var cmd = conn.CreateCommand()) {
                        try {
                            var sql = this.Config.GetSqlText();
                            foreach (var param in this.Config.SqlParameterList) {
                                object val = null;
                                if (param.IsVariable) {
                                    if (param.DataRowExists(srcrow)) {
                                        val = param.GetValueFromDataRow(srcrow);
                                    } else {
                                        val = this.Config.GetParameter(param.VariableName);
                                    }
                                } else {
                                    val = param.Value;
                                }
                                if (param.IsPlaceHolderParameter) {
                                    cmd.Parameters.Add(param.Name, this.GetDataType(param.DataType)).Value = val;
                                }
                                if (param.IsReplaceParameter) {
                                    sql = sql.Replace("{" + param.Name + "}", (string)val);
                                }
                            }
                            cmd.CommandText = sql;
                            cmd.CommandTimeout = 0;
                            //this.Logger.DebugFormat("SQL:{0}", cmd.CommandText);
                            using (var adapter = new OdbcDataAdapter()) {
                                adapter.SelectCommand = cmd;
                                adapter.Fill(ldtbl);
                            }
                        } catch (Exception ex) {
                            this.Logger.Error(ex,"Load Join Data Error:");
                            this.Logger.Error("SQL:{0}", cmd.CommandText);
                            foreach (OdbcParameter param in cmd.Parameters) {
                                this.Logger.Error("{0}={1}", param.ParameterName,param.Value);
                            }
                        }
                    }

                    //1件目に全データ定義がそろうので出力DataTableの定義を行う
                    if (outtbl.Columns.Count == 0) {
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
                    }

                    //出力DataTableのセット
                    if (ldtbl.Rows.Count > 0) {
                        bool firstflg = true;
                        foreach (DataRow ldrow in ldtbl.Rows) {
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
                conn.Close();
            }

            this.Config.SetDataTable(outtbl);
        }

        private OdbcConnection GetConnection() {
            DbConnectionFactory dbf = new DbConnectionFactory(this.Config.ConnectionName);
            return dbf.CreateOdbc();
        }

        public OdbcType GetDataType(string typename) {
            OdbcType datatype = OdbcType.NVarChar;
            if (typename == "Date") {
                datatype = OdbcType.Date;
            } else if (typename == "Time") {
                datatype = OdbcType.Time;
            }
            return datatype;
        }

    }
}
