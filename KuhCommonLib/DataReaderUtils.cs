using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KuhCommonLib
{
    public class DataReaderUtils
    {
        public IDataReader Reader { get; private set; }

        public DataReaderUtils(IDataReader reader) {
            this.Reader = reader;
        }

        public int? GetRowValueInt(string colname) {
            if (this.IsNull(colname)) return null;

            var idx = this.Reader.GetOrdinal(colname);
            Object inobj = this.Reader.GetValue(idx);

            if (inobj == null) return null;
            if (inobj.GetType() == typeof(System.DBNull)) return null;
            string str = inobj.ToString();
            int val = 0;
            if (int.TryParse(str, out val)) {
                return val;
            } else {
                return null;
            }
        }

        public long? GetRowValueLong(string colname) {
            if (this.IsNull(colname)) return null;

            var idx = this.Reader.GetOrdinal(colname);
            Object inobj = this.Reader.GetValue(idx);

            if (inobj == null) return null;
            if (inobj.GetType() == typeof(System.DBNull)) return null;
            string str = inobj.ToString();
            long val = 0;
            if (long.TryParse(str, out val)) {
                return val;
            } else {
                return null;
            }
        }

        public Decimal? GetRowValueDecimal(string colname) {
            if (this.IsNull(colname)) return null;

            var idx = this.Reader.GetOrdinal(colname);
            Object inobj = this.Reader.GetValue(idx);

            if (inobj == null) return null;
            if (inobj.GetType() == typeof(System.DBNull)) return null;
            string str = inobj.ToString();
            Decimal val = 0;
            if (Decimal.TryParse(str, out val)) {
                return val;
            } else {
                return null;
            }
        }

        public DateTime? GetRowValueDate(string colname) {
            if (this.IsNull(colname)) return null;

            var idx = this.Reader.GetOrdinal(colname);
            Object inobj = this.Reader.GetValue(idx);
            if (inobj == null) return null;
            if (inobj.GetType() == typeof(System.DBNull)) return null;
            if (inobj.GetType() == typeof(System.DateTime)) {
                return (DateTime)inobj;
            } else if (inobj.GetType() == typeof(System.TimeSpan)) {
                return DateTime.Parse(inobj.ToString());
            } else {
                var str = this.GetRowValueString(colname);
                DateTime dt;
                if (DateTime.TryParse(str, out dt)) {
                    return dt;
                } else {
                    throw new InvalidCastException("DataRowのデータをDateTimeに変換できません。" + inobj.ToString());
                }
            }
        }

        public DateTime? GetRowValueDateTime(string datecolname, string timecolname) {
            DateTime? dt = this.GetRowValueDate(datecolname);
            DateTime? tm = this.GetRowValueDate(timecolname);
            if (dt.HasValue && tm.HasValue) {
                return new DateTime(dt.Value.Year, dt.Value.Month, dt.Value.Day, tm.Value.Hour, tm.Value.Minute, tm.Value.Second);
            } else if (dt.HasValue && !tm.HasValue) {
                return new DateTime(dt.Value.Year, dt.Value.Month, dt.Value.Day, 0, 0, 0);
            } else if (!dt.HasValue && tm.HasValue) {
                return new DateTime(1, 1, 1, tm.Value.Hour, tm.Value.Minute, tm.Value.Second);
            } else {
                throw new InvalidCastException("DataRowのデータをDateTimeに変換できません。" + datecolname + "," + timecolname);
            }
        }

        public string GetRowValueString(string colname) {
            var idx = this.Reader.GetOrdinal(colname);

            if (this.Reader.IsDBNull(idx)) return "";
            if (this.Reader.GetValue(idx) == null) return "";

            string str = "";
            if(this.Reader.GetFieldType(idx) == typeof(string)) {
                str = this.Reader.GetString(idx);
            } else {
                str = this.Reader.GetValue(idx).ToString();
            }

            //電子カルテDB2の文字コード不整合に伴う補正
            str = str.Replace(Convert.ToChar(8722), 'ー');
            str = str.Replace(Convert.ToChar(12316), '～');

            return str;
        }

        public bool IsNull(string colname) {
            var idx = this.Reader.GetOrdinal(colname);

            if (this.Reader.IsDBNull(idx)) return true;
            if (this.Reader.GetValue(idx)==null) return true;
            if (string.IsNullOrWhiteSpace(this.GetRowValueString(colname))) {
                return true;
            } else {
                return false;
            }
        }

        public bool HasRow {
            get {
                if (this.Reader != null) {
                    return true;
                } else {
                    return false;
                }
            }
        }


    }
}
