using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.Odbc;
using System.Configuration;

namespace KuhCommonLib
{
    public class DbConnectionFactory : IDisposable
    {
        private string DatabaseTarget;

        private SortedList<string, OdbcConnection> ConnectionList;

        public DbConnectionFactory(string dbtarget) {
            this.DatabaseTarget = dbtarget;
            this.ConnectionList = new SortedList<string, OdbcConnection>();
        }

        public OdbcConnection CreateOdbc() {
            var section = (ClientSettingsSection)ConfigurationManager.GetSection("applicationSettings/KuhCommonLib.Properties.Settings");
            var connstr = section.Settings.Get("ConnectionString_" + this.DatabaseTarget).Value.ValueXml.InnerText;
            OdbcConnection conn = new OdbcConnection(connstr);
            conn.ConnectionTimeout = 0;

            if (this.ConnectionList.ContainsKey(this.DatabaseTarget)) {
                this.ConnectionList[this.DatabaseTarget] = conn;
            } else {
                this.ConnectionList.Add(this.DatabaseTarget, conn);
            }

            return conn;
        }

        public OdbcConnection GetOdbc() {
            return this.ConnectionList[this.DatabaseTarget];
        }

        public string GetPrefixString() {
            string prefix = "";
            return prefix;
        }

        #region IDisposable Support
        private bool disposedValue = false; // 重複する呼び出しを検出するには

        protected virtual void Dispose(bool disposing) {
            if (!disposedValue) {
                if (disposing) {
                    // TODO: マネージ状態を破棄します (マネージ オブジェクト)。
                }

                // TODO: アンマネージ リソース (アンマネージ オブジェクト) を解放し、下のファイナライザーをオーバーライドします。
                // TODO: 大きなフィールドを null に設定します。
                foreach (OdbcConnection conn in ConnectionList.Values) {
                    conn.Close();
                    conn.Dispose();
                }
                this.ConnectionList.Clear();
                this.ConnectionList = null;

                disposedValue = true;
            }
        }

        // TODO: 上の Dispose(bool disposing) にアンマネージ リソースを解放するコードが含まれる場合にのみ、ファイナライザーをオーバーライドします。
        // ~DbConnectionFactory() {
        //   // このコードを変更しないでください。クリーンアップ コードを上の Dispose(bool disposing) に記述します。
        //   Dispose(false);
        // }

        // このコードは、破棄可能なパターンを正しく実装できるように追加されました。
        public void Dispose() {
            // このコードを変更しないでください。クリーンアップ コードを上の Dispose(bool disposing) に記述します。
            Dispose(true);
            // TODO: 上のファイナライザーがオーバーライドされる場合は、次の行のコメントを解除してください。
            // GC.SuppressFinalize(this);
        }
        #endregion

    }
}
