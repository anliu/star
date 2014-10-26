using Microsoft.HBase.Client.Common;
using Microsoft.HBase.Client.Utilities;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
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
        }

        private void btOK_Click(object sender, EventArgs e)
        {
            SetMetaData();
        }

        private void SetMetaData()
        {
            var outputMain = md.OutputCollection[0].IsErrorOut ? md.OutputCollection[1] : md.OutputCollection[0];

            try
            {
                var propList = JsonParser.GetPropertyList(this.tbMapping.Text);

                // input is valid, set the property
                var propMapping = md.CustomPropertyCollection[Constants.PropMapping];
                if (propMapping != null)
                {
                    propMapping.Value = this.tbMapping.Text;
                }

                // clear the current output columns
                outputMain.OutputColumnCollection.RemoveAll();

                foreach (var prop in propList)
                {
                    var outputCol = outputMain.OutputColumnCollection.New();
                    outputCol.Name = prop;
                    outputCol.SetDataTypeProperties(SqlServer.Dts.Runtime.Wrapper.DataType.DT_WSTR, 50, 0, 0, 0);
                }
            }
            catch (JsonException)
            {
            }
        }
    }
}
