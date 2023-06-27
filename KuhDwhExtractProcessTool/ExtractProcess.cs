using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KuhDwhExtractProcessTool
{
    class ExtractProcess
    {
        private NLog.ILogger Logger;
        private List<ExtractPhase> PhaseList;

        private DataSet WorkingDataSet;

        private SortedList<string, string> ParameterList;

        public ExtractProcess(string configfilename) {
            this.Logger = NLog.LogManager.GetLogger(this.GetType().ToString());
            this.Logger.Info("Initialize ExtractProcess");

            this.PhaseList = new List<ExtractPhase>();
            this.ReadConfig(configfilename);

            this.WorkingDataSet = new DataSet();
            this.ParameterList = new SortedList<string, string>();
        }

        public void SetParameter(string name,string val) {
            if (this.ParameterList.ContainsKey(name)) {
                this.ParameterList[name] = val;
            } else {
                this.ParameterList.Add(name, val);
            }
        }

        public void AddPhase(ExtractPhase phase) {
            this.PhaseList.Add(phase);
        }

        public void Run() {
            this.Logger.Info("Start ExtractProcess");

            int no = 1;
            foreach (var phase in this.PhaseList) {
                this.Logger.Info("- Step {0}:{1}", no,phase.ActionType);
                phase.SetWorkingDataSet(this.WorkingDataSet);
                phase.SetParameterList(this.ParameterList);
                phase.DoAction();
                no++;
            }

            this.Logger.Info("End ExtractProcess");
        }

        public void ReadConfig(string filename) {
            this.Logger.Info("- Read Config File:{0}", filename);

            using(var reader=new System.IO.StreamReader(filename, Encoding.UTF8)) {
                var jsontext = reader.ReadToEnd();
                using(var stream=new System.IO.MemoryStream(Encoding.UTF8.GetBytes(jsontext))) {
                    var seri = new System.Runtime.Serialization.Json.DataContractJsonSerializer(typeof(List<ExtractPhase>));
                    this.PhaseList = (List<ExtractPhase>)seri.ReadObject(stream);
                }
            }
        }

        public void WriteConfig(string filename) {
            this.Logger.Info("- Write Config File:{0}", filename);

            var setting = new System.Runtime.Serialization.Json.DataContractJsonSerializerSettings() {
                UseSimpleDictionaryFormat=true
            };
            var seri = new System.Runtime.Serialization.Json.DataContractJsonSerializer(typeof(List<ExtractPhase>));
            using (var stream = new System.IO.FileStream(filename, System.IO.FileMode.Create)) {
                seri.WriteObject(stream, this.PhaseList);
                stream.Flush();
            }

        }
    }
}
