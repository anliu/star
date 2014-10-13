using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.HBase.Client
{
    [
        DtsPipelineComponent(ComponentType = ComponentType.SourceAdapter,
            DisplayName = "HBase Source Component",
            Description = "HBase Source Component")
    ]
    public class HBaseSrc : PipelineComponent
    {
        public override void ProvideComponentProperties()
        {
            // Reset the component.
            base.RemoveAllInputsOutputsAndCustomProperties();

            // start out clean, remove anything put on by the base class
            RemoveAllInputsOutputsAndCustomProperties();

            // error dispositions
            // ComponentMetaData.UsesDispositions = true;

            IDTSRuntimeConnection100 connection = ComponentMetaData.RuntimeConnectionCollection.New();
            connection.Name = "HBase";

            // Add the command timeout property.
            var propTableName = ComponentMetaData.CustomPropertyCollection.New();
            propTableName.Name = "Table";
            propTableName.Description = "HBase table name";
            propTableName.Value = string.Empty;

            IDTSOutput100 output = ComponentMetaData.OutputCollection.New();
            output.Name = "Output";

            //ComponentMetaData.OutputCollection[0].ExternalMetadataColumnCollection.IsUsed = true;

            //// Insert an error output.
            //AddErrorOutput(Localized.ErrorOutputName, 0, 0);

            //// Set we want to validate external metadata
            //ComponentMetaData.ValidateExternalMetadata = true;
        }

        public override void AcquireConnections(object transaction)
        {
            if (ComponentMetaData.RuntimeConnectionCollection[0].ConnectionManager != null)
            {
                ConnectionManager cm = DtsConvert.GetWrapper(ComponentMetaData.RuntimeConnectionCollection[0].ConnectionManager);
                HBaseConnectionManager cmhbase = cm.InnerObject as HBaseConnectionManager;

                if (cmhbase == null)
                {
                    throw new Exception("The ConnectionManager " + cm.Name + " is not an HBase connection.");
                }

                //var hbaseConnection = cmhbase.AcquireConnection(transaction) as SqlConnection;
                //hbaseConnection.Open();
            }
        }

        public override void ReleaseConnections()
        {
            //if (hbaseConnection != null && hbaseConnection.State != ConnectionState.Closed)
            //    hbaseConnection.Close();
        }
    }
}
