using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace KuhDwhExtractProcessTool
{
    [DataContract]
    class ExtractPhase
    {
        [DataMember]
        public string ActionType { get; set; }
        [DataMember]
        public string OutputName { get; set; }

        [DataMember]
        public string ConnectionName { get; set; }
        [DataMember]
        public string SqlText { get; set; }
        [DataMember]
        public List<string> SqlTextLines { get; set; }
        [DataMember]
        public List<SqlParameter> SqlParameterList { get; set; }

        [DataMember]
        public string WebServiceUrl { get; set; }
        [DataMember]
        public List<WebServiceParameter> WebServiceParameterList { get; set; }
        [DataMember]
        public string WebServiceFilterExpression { get; set; }

        [DataMember]
        public string JoinType { get; set; }
        [DataMember]
        public List<JoinColumn> JoinColumnList { get; set; }
        [DataMember]
        public string SourceName { get; set; }
        [DataMember]
        public string FileName { get; set; }
        [DataMember]
        public string Encoding { get; set; }
        [DataMember]
        public string FilterExpression { get; set; }
        [DataMember]
        public List<FilterParameter> FilterParameterList { get; set; }
        [DataMember]
        public string SortExpression { get; set; }

        [DataMember]
        public List<string> KeyColumnList { get; set; }
        [DataMember]
        public List<FlattenColumn> FlattenColumnList { get; set; }

        [DataMember]
        public List<ConvertColumn> ConvertColumnList { get; set; }

        public bool? NoDataOutputFile { get; set; }

        private DataSet WorkingDataSet;
        private SortedList<string, string> ParameterList;

        public ExtractPhase() {
            this.SqlParameterList = new List<SqlParameter>();
            this.WebServiceParameterList = new List<WebServiceParameter>();
            this.JoinColumnList = new List<JoinColumn>();
            this.FilterParameterList = new List<FilterParameter>();
            this.KeyColumnList = new List<string>();
            this.FlattenColumnList = new List<FlattenColumn>();
        }

        public void SetParameterList(SortedList<string,string> paramlst) {
            this.ParameterList = paramlst;
        }
        public string GetParameter(string name) {
            return this.ParameterList[name];
        }

        public bool ContainsParameter(string name) {
            if (this.ParameterList.ContainsKey(name)) {
                return true;
            }
            return false;
        }

        public void SetWorkingDataSet(DataSet wkds) {
            this.WorkingDataSet = wkds;
        }
        public void SetDataTable(DataTable tbl) {
            this.WorkingDataSet.Tables.Add(tbl);
        }
        public DataTable GetDataTable(string tblname) {
            return this.WorkingDataSet.Tables[tblname];
        }

        public DataTable GetSourceDataTable() {
            return this.WorkingDataSet.Tables[this.SourceName];
        }

        public void DoAction() {
            switch (this.ActionType) {
                case "LoadFromCSV":
                    var loader1 = new CsvDataReader(this);
                    loader1.Load();
                    break;
                case "LoadFromODBC":
                    var loader2 = new OdbcDataLoader(this);
                    loader2.Load();
                    break;
                case "LoadJoinFromODBC":
                    var loader3 = new OdbcDataLoader(this);
                    loader3.LoadAndJoin();
                    break;
                case "LoadJoinFromWebService":
                    var loader4 = new WebServiceDataLoader(this);
                    loader4.LoadAndJoin();
                    break;
                case "LoadJoinFromCSV":
                    var loader5 = new CsvDataReader(this);
                    loader5.LoadAndJoin();
                    break;
                case "LoadMergeFromCSV":
                    var loader6 = new CsvDataReader(this);
                    loader6.LoadAndMerge();
                    break;
                case "FilterInclusion":
                    var filter1 = new InclusionDataFilter(this);
                    filter1.Filter();
                    break;
                case "FlattenTable":
                    var conv1 = new FlattenTable(this);
                    conv1.Flatten();
                    break;
                case "ConvertTable":
                    var conv2 = new TableDataConverter(this);
                    conv2.Convert();
                    break;
                case "WriteToCsvFile":
                    var writer = new CsvDataWriter(this);
                    writer.Write();
                    break;
                default:
                    break;
            }
        }

        public string GetSqlText() {
            if (string.IsNullOrWhiteSpace(this.SqlText)) {
                var sql = new StringBuilder();
                this.SqlTextLines.ForEach(i => {
                    sql.AppendLine(i);
                });
                return sql.ToString();
            } else {
                return this.SqlText;
            }
        }

        public string GetKeyColumnNameCsvString() {
            var str = new StringBuilder();
            this.KeyColumnList.ForEach(i => {
                if (str.Length > 0) {
                    str.Append(",");
                }
                str.Append(i);
            });
            return str.ToString();
        }

        public bool IsSelectEncoding() {
            if (string.IsNullOrWhiteSpace(this.Encoding)) {
                return false;
            } else {
                return true;
            }
        }

        [DataContract]
        public class SqlParameter
        {
            [DataMember]
            public string Name { get; set; }
            [DataMember]
            public string ParameterType { get; set; }
            [DataMember]
            public string DataType { get; set; }
            [DataMember]
            public string Value { get; set; }
            [DataMember]
            public string Format { get; set; }

            public bool IsReplaceParameter {
                get {
                    if (this.ParameterType == "Replace") {
                        return true;
                    } else {
                        return false;
                    }
                }
            }
            public bool IsPlaceHolderParameter {
                get {
                    if (this.ParameterType == "Replace") {
                        return false;
                    } else {
                        return true;
                    }
                }
            }

            public bool IsVariable {
                get {
                    if (this.Value.StartsWith("@")) {
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            public string VariableName {
                get {
                    return this.Value.Substring(1);
                }
            }

            public bool DataRowExists(DataRow row) {
                var colname = this.VariableName;
                if (row.Table.Columns.Contains(colname)) {
                    return true;
                } else {
                    return false;
                }
            }

            public object GetValueFromDataRow(DataRow row) {
                object val = null;
                var colname = this.VariableName;
                val = row[colname];

                if (val is DBNull) {
                    val = "";
                }
                if (!string.IsNullOrWhiteSpace(this.Format)) {
                    val = string.Format("{0:" + this.Format + "}", val);
                }
                return val;
            }
        }

        [DataContract]
        public class WebServiceParameter
        {
            [DataMember]
            public string Name { get; set; }
            [DataMember]
            public string DataType { get; set; }
            [DataMember]
            public string Value { get; set; }
            [DataMember]
            public string Format { get; set; }

            public bool IsVariable {
                get {
                    if (this.Value.StartsWith("@")) {
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            public string VariableName {
                get {
                    return this.Value.Substring(1);
                }
            }

            public object GetValueFromDataRow(DataRow row) {
                object val = null;
                if (this.IsVariable) {
                    var colname = this.VariableName;
                    val = row[colname];
                    if (!string.IsNullOrWhiteSpace(this.Format)) {
                        val = string.Format("{0:" + this.Format + "}", val);
                    }
                } else {
                    val = this.Value;
                }
                return val;
            }
        }

        [DataContract]
        public class FilterParameter
        {
            [DataMember]
            public string Name { get; set; }
            [DataMember]
            public string DataType { get; set; }
            [DataMember]
            public string Value { get; set; }
            [DataMember]
            public string Format { get; set; }

            public bool IsVariable {
                get {
                    if (this.Value.StartsWith("@")) {
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            public string VariableName {
                get {
                    return this.Value.Substring(1);
                }
            }

            public bool IsQuotationMark {
                get {
                    var dp = this.DataType.ToLower();
                    if (dp == "string" || dp=="date") {
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            public bool DataRowExists(DataRow row) {
                var colname = this.VariableName;
                if (row.Table.Columns.Contains(colname)) {
                    return true;
                } else {
                    return false;
                }
            }

            public object GetValueFromDataRow(DataRow row) {
                object val = null;
                var colname = this.VariableName;
                val = row[colname];

                if (val is DBNull) {
                    val = "";
                }
                if (!string.IsNullOrWhiteSpace(this.Format)) {
                    val = string.Format("{0:" + this.Format + "}", val);
                }
                return val;
            }
        }

        [DataContract]
        public class JoinColumn
        {
            [DataMember]
            public string SourceTableName { get; set; }
            [DataMember]
            public string SourceColumnName { get; set; }
            [DataMember]
            public string DestinationColumnName { get; set; }
            [DataMember]
            public string DestinationColumnType { get; set; }
            [DataMember]
            public string ParseFormat { get; set; }

            public Type GetDestinationColumnType() {
                Type datatype = typeof(string);
                if (DestinationColumnType == "Date") {
                    datatype = typeof(DateTime);
                } else if (DestinationColumnType == "Time") {
                    datatype = typeof(TimeSpan);
                } else if(DestinationColumnType == "Integer") {
                    datatype = typeof(int);
                } else if (DestinationColumnType == "Number") {
                    datatype = typeof(Decimal);
                }
                return datatype;
            }

            public bool IsDefinedType {
                get {
                    if (string.IsNullOrWhiteSpace(this.DestinationColumnType)) {
                        return false;
                    } else {
                        return true;
                    }
                }
            }

            public object ConvertDestinationValue(string inval) {
                object ret = DBNull.Value;
                if (string.IsNullOrWhiteSpace(inval)) {
                    return ret;
                }

                var detp = this.GetDestinationColumnType();
                if(detp == typeof(int)) {
                    int retint;
                    if(int.TryParse(inval,out retint)) {
                        ret = retint;
                    }
                } else if (detp == typeof(Decimal)) {
                    Decimal retnum;
                    if(Decimal.TryParse(inval,out retnum)) {
                        ret = retnum;
                    }
                } else if (detp == typeof(DateTime)) {
                    DateTime retdt;
                    if(DateTime.TryParse(inval,out retdt)) {
                        ret = retdt;
                    }
                } else if (detp == typeof(TimeSpan)) {
                    TimeSpan retdt;
                    if (TimeSpan.TryParse(inval, out retdt)) {
                        ret = retdt;
                    }
                } else {
                    ret = inval;
                }
                return ret;
            }
        }

        [DataContract]
        public class FlattenColumn
        {
            [DataMember]
            public string DestinationColumnName { get; set; }
            [DataMember]
            public string KeyColumnName { get; set; }
            [DataMember]
            public string KeyColumnValue { get; set; }
            [DataMember]
            public string ValueColumnName { get; set; }

        }

        [DataContract]
        public class ConvertColumn
        {
            [DataMember]
            public string ConvertType { get; set; }
            [DataMember]
            public string DestinationColumnName { get; set; }
            [DataMember]
            public string DestinationColumnType { get; set; }
            [DataMember]
            public string SourceColumnName { get; set; }
            [DataMember]
            public string Format { get; set; }
            [DataMember]
            public List<ConvertFunctionParameter> FunctionParameterList { get; set; }

            public Type GetDestinationColumnType() {
                Type datatype = typeof(string);
                if (DestinationColumnType == "Date") {
                    datatype = typeof(DateTime);
                } else if (DestinationColumnType == "Time") {
                    datatype = typeof(TimeSpan);
                } else if (DestinationColumnType == "Integer") {
                    datatype = typeof(int);
                } else if (DestinationColumnType == "Number") {
                    datatype = typeof(Decimal);
                }
                return datatype;
            }

            public object GetFormatValueFromDataRow(DataRow row) {
                object val = null;
                val = row[this.SourceColumnName];

                if (val is DBNull) {
                    val = "";
                }
                if (!string.IsNullOrWhiteSpace(this.Format)) {
                    val = string.Format("{0:" + this.Format + "}", val);
                }
                return val;
            }

            public object GetFunctionValueFromDataRow(DataRow row) {
                object ret=null;
                switch (this.ConvertType) {
                    case "Age":
                        try {
                            DateTime birthday = (DateTime)this.FunctionParameterList[0].GetValueFromDataRow(row);
                            DateTime curdt = (DateTime)this.FunctionParameterList[1].GetValueFromDataRow(row);
                            int age = curdt.Year - birthday.Year;
                            if (birthday > curdt.AddYears(-age)) {
                                age = age - 1;
                            }
                            ret = age;
                        } catch (InvalidCastException ex) {
                            Console.WriteLine("Age Convert Error:{0}",ex.ToString());
                            ret = null;
                        }
                        break;
                    case "Concatenate":
                        var str = new StringBuilder();
                        foreach (var inval in this.FunctionParameterList) {
                            str.Append(inval.GetValueFromDataRow(row));
                        }
                        ret = str.ToString();
                        break;
                    case "SetUnkownDate":
                        DateTime indt = (DateTime)this.FunctionParameterList[0].GetValueFromDataRow(row);
                        ret = indt;
                        foreach (var inval in (from i in this.FunctionParameterList where i.Name.Contains("UNKOWNVAL") select i)) {
                            string strchkdt= (string)inval.GetValueFromDataRow(row);
                            DateTime chkdt = DateTime.Parse(strchkdt);
                            if (indt == chkdt) {
                                ret = new DateTime(2100,1,1);
                                break;
                            }
                        }
                        break;
                    case "Hash":
                        string instr = (string)this.FunctionParameterList[0].GetValueFromDataRow(row);
                        string fmt= (string)this.FunctionParameterList[1].GetValueFromDataRow(row);
                        string srcstr = string.Format(fmt, instr);
                        byte[] srcbytes = System.Text.Encoding.UTF8.GetBytes(srcstr);

                        var hash = new System.Security.Cryptography.SHA256Cng();
                        byte[] distbytes = hash.ComputeHash(srcbytes);
                        StringBuilder hashstr = new StringBuilder();
                        foreach (var b in distbytes) {
                            hashstr.AppendFormat("{0:X2}", b);
                        }
                        ret = hashstr.ToString();
                        break;
                    default:
                        break;
                }
                return ret;
            }
        }

        [DataContract]
        public class ConvertFunctionParameter
        {
            [DataMember]
            public string Name { get; set; }
            [DataMember]
            public string DataType { get; set; }
            [DataMember]
            public string Value { get; set; }
            [DataMember]
            public string Format { get; set; }

            public bool IsVariable {
                get {
                    if (this.Value.StartsWith("@")) {
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            public string VariableName {
                get {
                    return this.Value.Substring(1);
                }
            }

            public bool IsQuotationMark {
                get {
                    var dp = this.DataType.ToLower();
                    if (dp == "string" || dp == "date") {
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            public bool DataRowExists(DataRow row) {
                var colname = this.VariableName;
                if (row.Table.Columns.Contains(colname)) {
                    return true;
                } else {
                    return false;
                }
            }

            public object GetValueFromDataRow(DataRow row) {
                object val;
                if (!this.IsVariable) {
                    val = this.Value;
                    return val;
                }
                var colname = this.VariableName;
                val = row[colname];

                if (val is DBNull) {
                    val = "";
                }
                if (!string.IsNullOrWhiteSpace(this.Format)) {
                    val = string.Format("{0:" + this.Format + "}", val);
                }
                return val;
            }
        }
    }
}
