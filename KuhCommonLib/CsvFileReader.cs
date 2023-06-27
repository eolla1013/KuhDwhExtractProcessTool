using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace KuhCommonLib
{
    public class CsvFileReader
    {
        public DataTable CsvData { get; private set; }
        public bool HeaderExists { get; set; }

        private System.Text.Encoding FileEncoding;

        public CsvFileReader() {
            this.CsvData = new DataTable();
            this.FileEncoding = System.Text.Encoding.GetEncoding("Shift_JIS");
            this.HeaderExists = false;
        }

        public CsvFileReader(string encname) {
            this.CsvData = new DataTable();
            this.FileEncoding = System.Text.Encoding.GetEncoding(encname);
            this.HeaderExists = false;
        }

        public void Read(string srcfilename) {
            this.CsvData.Clear();
            using (var rdr = new System.IO.StreamReader(srcfilename, this.FileEncoding)) {
                List<String> row = new List<string>();
                StringBuilder cellstr = new StringBuilder();
                bool wqflg = false;
                bool wqflg2 = false;
                //bool crflg;
                int colidx = 0;
                int rowidx = 0;
                while (!rdr.EndOfStream) {
                    int cd = rdr.Read();
                    string chr = Char.ConvertFromUtf32(cd);
                    //System.Diagnostics.Debug.Print(cd + ":" + chr);
                    if (chr == "\"") {  //データ中かどうかの判定
                        if (wqflg) {
                            if (wqflg2) {   //ダブルクォーテーションのエスケープ処理
                                wqflg2 = false;
                                cellstr.Append(chr);
                            } else {
                                wqflg2 = true;
                            }
                        } else {
                            wqflg = true;
                            wqflg2 = false;
                        }
                    } else if ((wqflg==false || wqflg==true && wqflg2==true) && chr == ",") {  //区切り文字かどうかの判定、データの終わり処理
                        //System.Diagnostics.Debug.Print("(" + rowidx + "," + colidx + ")=" + cellstr.ToString());
                        row.Add(cellstr.ToString());
                        colidx++;
                        cellstr.Clear();
                        wqflg = false;
                        wqflg2 = false;
                    } else if ((wqflg == false || wqflg == true && wqflg2 == true) && chr == "\r") {   //CRの判定
                        //crflg = true;
                    } else if ((wqflg == false || wqflg == true && wqflg2 == true) && chr == "\n") {   //LFの判定、行の終わり処理
                        //System.Diagnostics.Debug.Print("(" + rowidx + "," + colidx + ")=" + cellstr.ToString());
                        row.Add(cellstr.ToString());
                        colidx++;
                        cellstr.Clear();
                        wqflg = false;
                        wqflg2 = false;

                        //カラムがなかったときは最初の1行でカラムを生成
                        bool firstrow = false;
                        if (this.CsvData.Columns.Count == 0) {
                            if (this.HeaderExists) {
                                //ヘッダがあったとき
                                for (int i = 0; i < row.Count; i++) {
                                    this.CsvData.Columns.Add(row[i], typeof(String)); 
                                }
                                firstrow = true;
                            } else {
                                //ヘッダがないときはColumn{nnn}
                                for (int i = 0; i < row.Count; i++) {
                                    this.CsvData.Columns.Add("Column" + (i + 1).ToString("000"), typeof(String));
                                }
                            }
                        }
                        if (!firstrow) {
                            var outrow = this.CsvData.NewRow();
                            for (int i = 0; i < row.Count; i++) {
                                outrow[i] = row[i];
                            }
                            this.CsvData.Rows.Add(outrow);
                        }

                        rowidx++;
                        row = new List<string>();
                        colidx=0;

                        //crflg = false;
                    } else {
                        cellstr.Append(chr);
                    }
                }
            }
        }
    }
}
