using Microsoft.HBase.Client.Common;
using Microsoft.HBase.Client.Utilities;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
//using Microsoft.SqlServer.Dts.Runtime.Wrapper;
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
                int i = 0;

                // input is valid, set the property
                var propMapping = md.CustomPropertyCollection[Constants.PropMapping];
                if (propMapping != null)
                {
                    propMapping.Value = this.tbMapping.Text;
                }

                foreach (var prop in propList)
                {
                    var typeName = (typeNameProp.Value ?? string.Empty).ToString();
                    if (typeName.IndexOf("TxJSONDerived") >= 0)
                    {
                        var outputMain = md.OutputCollection[0].IsErrorOut ? md.OutputCollection[1] : md.OutputCollection[0];
                        // clear the current output columns
                        outputMain.OutputColumnCollection.RemoveAll();
                        var outputCol = outputMain.OutputColumnCollection.NewAt(i++);
                        outputCol.Name = prop;
                        outputCol.SetDataTypeProperties(SqlServer.Dts.Runtime.Wrapper.DataType.DT_WSTR, 50, 0, 0, 0);
                    }
                    else if (typeName.IndexOf("TxDerivedJSON") >= 0)
                    {
                        var inputMain = md.InputCollection[0];
                        // clear the current input columns
                        // inputMain.InputColumnCollection.RemoveAll();
                        inputMain.ExternalMetadataColumnCollection.RemoveAll();
                        var externCol = inputMain.ExternalMetadataColumnCollection.NewAt(i++);
                        externCol.Name = prop;
                        //externCol.DataType = DataType.DT_WSTR;
                        externCol.Length = 50;
                        externCol.Precision = 0;
                        externCol.Scale = 0;
                        externCol.CodePage = 0;
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
