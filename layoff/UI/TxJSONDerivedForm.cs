using Microsoft.HBase.Client.Common;
using Microsoft.HBase.Client.Utilities;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Microsoft.HBase.Client.UI
{
    public partial class TxJSONDerivedForm : Form
    {
        private IDTSComponentMetaData100 md;

        public TxJSONDerivedForm( Connections cons, Variables vars, IDTSComponentMetaData100 md)
        {
            InitializeComponent();
            this.md = md;

            var propMapping = md.CustomPropertyCollection[Constants.PropMapping];
            if (propMapping != null)
            {
                this.tbMapping.Text = (string)propMapping.Value;
            }
        }

        private void btOK_Click(object sender, EventArgs e)
        {
            SetMetaData();
        }

        private void SetMetaData()
        {
            var typeNameProp = md.CustomPropertyCollection["UserComponentTypeName"];
            if (typeNameProp == null)
            {
                // error out, should not happen unless SSIS breaks us
                throw new Exception("Builtin property broken.");
            }

            try
            {
                var propList = JsonParser.GetPropertyList(this.tbMapping.Text);
                bool derivedJson = false, jsonDerived = false;
                var mapping = new Dictionary<string, int>();
                int i;

                // input is valid, set the property
                var propMapping = md.CustomPropertyCollection[Constants.PropMapping];
                if (propMapping != null)
                {
                    propMapping.Value = this.tbMapping.Text;
                }

                var typeName = (typeNameProp.Value ?? string.Empty).ToString();
                if (typeName.IndexOf("TxJSONDerived") >= 0)
                {
                    // clear the current output columns
                    var outputMain = md.OutputCollection[0].IsErrorOut ? md.OutputCollection[1] : md.OutputCollection[0];
                    outputMain.OutputColumnCollection.RemoveAll();
                    jsonDerived = true;
                }
                else if (typeName.IndexOf("TxDerivedJSON") >= 0)
                {
                    var inputMain = md.InputCollection[0];
                    // clear the current input columns
                    // inputMain.InputColumnCollection.RemoveAll();
                    for (i = 0; i < inputMain.InputColumnCollection.Count; i++)
                    {
                        var externCol = inputMain.ExternalMetadataColumnCollection.FindObjectByID(inputMain.InputColumnCollection[i].ExternalMetadataColumnID);
                        mapping[externCol.Name] = inputMain.InputColumnCollection[i].ID;
                    }

                    inputMain.ExternalMetadataColumnCollection.RemoveAll();
                    derivedJson = true;
                }

                i = 0;
                foreach (var prop in propList)
                {
                    if (jsonDerived)
                    {
                        var outputMain = md.OutputCollection[0].IsErrorOut ? md.OutputCollection[1] : md.OutputCollection[0];
                        var outputCol = outputMain.OutputColumnCollection.NewAt(i++);
                        outputCol.Name = prop;
                        outputCol.SetDataTypeProperties(DataType.DT_WSTR, 50, 0, 0, 0);
                    }
                    else if (derivedJson)
                    {
                        var inputMain = md.InputCollection[0];
                        var externCol = inputMain.ExternalMetadataColumnCollection.NewAt(i++);
                        externCol.Name = prop;
                        externCol.DataType = DataType.DT_WSTR;
                        externCol.Length = 50;
                        externCol.Precision = 0;
                        externCol.Scale = 0;
                        externCol.CodePage = 0;

                        // preserve the input mapping if possible
                        if (mapping.ContainsKey(prop))
                        {
                            inputMain.InputColumnCollection.FindObjectByID(mapping[prop]).ExternalMetadataColumnID = externCol.ID;
                        }
                    }
                    else
                    {
                        // error
                        throw new Exception("Invalid component type for this UI");
                    }
                }
            }
            catch (JsonException)
            {
            }
        }
    }
}
