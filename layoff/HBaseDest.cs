using Microsoft.HBase.Client.Common;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.HBase.Client
{
    [
        DtsPipelineComponent(ComponentType = ComponentType.DestinationAdapter,
            CurrentVersion = 4,
            DisplayName = "HBase Destination",
            Description = "HBase Destination Component",
            RequiredProductLevel = Microsoft.SqlServer.Dts.Runtime.Wrapper.DTSProductLevel.DTSPL_NONE)
    ]
    public class HBaseDest : PipelineComponent
    {
        private HBaseClient _hbaseClient;
        internal bool Connected { get; set; }

        public override void ProvideComponentProperties()
        {
            // Reset the component.
            base.RemoveAllInputsOutputsAndCustomProperties();

            // start out clean, remove anything put on by the base class
            RemoveAllInputsOutputsAndCustomProperties();

            // error dispositions
            ComponentMetaData.UsesDispositions = true;

            IDTSRuntimeConnection100 connection = ComponentMetaData.RuntimeConnectionCollection.New();
            connection.Name = "HBase";

            // Add the table name property.
            var propTableName = ComponentMetaData.CustomPropertyCollection.New();
            propTableName.Name = Constants.PropTableName;
            propTableName.Description = "HBase table name";
            propTableName.Value = string.Empty;

            // Add the columns property.
            var propColumns = ComponentMetaData.CustomPropertyCollection.New();
            propColumns.Name = Constants.PropColumns;
            propColumns.Description = "Columns to retrieve";
            propColumns.Value = string.Empty;

            // Add the columns property.
            var propColumnTypes = ComponentMetaData.CustomPropertyCollection.New();
            propColumnTypes.Name = Constants.PropColumnTypes;
            propColumnTypes.Description = "Type of columns to retrieve";
            propColumnTypes.Value = string.Empty;

            IDTSInput100 input = ComponentMetaData.InputCollection.New();
            input.Name = "Input";

            // Get the assembly version and set that as our current version.
            SetComponentVersion();

            ComponentMetaData.OutputCollection[0].ExternalMetadataColumnCollection.IsUsed = true;

            // Insert an error output.
            AddErrorOutput("ErrorOutput", 0, 0);

            // Set we want to validate external metadata
            ComponentMetaData.ValidateExternalMetadata = true;
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
                this._hbaseClient = cmhbase.AcquireConnection(transaction) as HBaseClient;
                //hbaseConnection.Open();

                this.Connected = true;
            }
        }

        public override void ReleaseConnections()
        {
            //if (hbaseConnection != null && hbaseConnection.State != ConnectionState.Closed)
            //    hbaseConnection.Close();

            // dispose or call CM ReleaseConnections if applicable
            this._hbaseClient = null;
            this.Connected = false;
        }

        /// <summary>
        /// Disallow inserting an input by throwing an error.
        /// </summary>
        /// <param name="insertPlacement">unused</param>
        /// <param name="inputID">unused</param>
        /// <returns>N/A</returns>
        public override IDTSInput100 InsertInput(DTSInsertPlacement insertPlacement, int inputID)
        {
            bool bCancel;

            ErrorSupport.FireError(HResults.DTS_E_CANTADDINPUT, out bCancel);
            throw new PipelineComponentHResultException(HResults.DTS_E_CANTADDINPUT);
        }

        /// <summary>
        /// Disallow inserting an output by throwing an error.
        /// </summary>
        /// <param name="insertPlacement">unused</param>
        /// <param name="outputID">unused</param>
        /// <returns>N/A</returns>
        public override IDTSOutput100 InsertOutput(DTSInsertPlacement insertPlacement, int outputID)
        {
            bool bCancel;

            ErrorSupport.FireError(HResults.DTS_E_CANTADDOUTPUT, out bCancel);
            throw new PipelineComponentHResultException(HResults.DTS_E_CANTADDOUTPUT);
        }

        /// <summary>
        /// Disallow deleting an input by throwing an error.
        /// </summary>
        /// <param name="inputID">N/A</param>
        public override void DeleteInput(int inputID)
        {
            bool bCancel;

            ErrorSupport.FireError(HResults.DTS_E_CANTDELETEINPUT, out bCancel);
            throw new PipelineComponentHResultException(HResults.DTS_E_CANTDELETEINPUT);
        }

        /// <summary>
        /// Disallow deleting an output by throwing an error.
        /// </summary>
        /// <param name="outputID">unused</param>
        public override void DeleteOutput(int outputID)
        {
            bool bCancel;

            ErrorSupport.FireError(HResults.DTS_E_CANTDELETEOUTPUT, out bCancel);
            throw new PipelineComponentHResultException(HResults.DTS_E_CANTDELETEOUTPUT);
        }

        /// <summary>
        /// Disallow inserting an output column by throwing an error.
        /// </summary>
        /// <param name="outputID">unused</param>
        /// <param name="outputColumnIndex">unused</param>
        /// <param name="name">unused</param>
        /// <param name="description">unused</param>
        /// <returns>N/A</returns>
        public override IDTSOutputColumn100 InsertOutputColumnAt(int outputID, int outputColumnIndex, string name, string description)
        {
            bool bCancel;

            ErrorSupport.FireError(HResults.DTS_E_CANTADDCOLUMN, out bCancel);
            throw new PipelineComponentHResultException(HResults.DTS_E_CANTADDCOLUMN);
        }

        /// <summary>
        /// Disallow deleting an output column by throwing an error.
        /// </summary>
        /// <param name="outputID">unused</param>
        /// <param name="outputColumnID">unused</param>
        public override void DeleteOutputColumn(int outputID, int outputColumnID)
        {
            bool bCancel;

            ErrorSupport.FireError(HResults.DTS_E_CANTDELETECOLUMN, out bCancel);
            throw new PipelineComponentHResultException(HResults.DTS_E_CANTDELETECOLUMN);
        }

        public override void ReinitializeMetaData()
        {
            // baseclass may have some work to do here
            base.ReinitializeMetaData();
        }

        public override DTSValidationStatus Validate()
        {
            var status = base.Validate();
            if (status == DTSValidationStatus.VS_ISCORRUPT)
            {
                return status;
            }

            // should have one input
            if (ComponentMetaData.InputCollection.Count != 1)
            {
                return DTSValidationStatus.VS_ISCORRUPT;
            }

            // should have no outputs except error output
            if (ComponentMetaData.OutputCollection.Count > 1)
            {
                return DTSValidationStatus.VS_ISCORRUPT;
            }

            // validate properties
            var propTableName = ComponentMetaData.CustomPropertyCollection[Constants.PropTableName];
            if (propTableName == null || propTableName.Value == null ||
                string.IsNullOrEmpty(propTableName.Value.ToString().Trim()))
            {
                return DTSValidationStatus.VS_ISCORRUPT;
            }

            var propColumns = ComponentMetaData.CustomPropertyCollection[Constants.PropColumns];
            if (propColumns == null || propColumns.Value == null ||
                string.IsNullOrEmpty(propColumns.Value.ToString().Trim()))
            {
                return DTSValidationStatus.VS_ISCORRUPT;
            }

            if (this.Connected && ComponentMetaData.ValidateExternalMetadata == true)
            {
                // validate table existence, columns are always available per
                // hbase dynamic column model
            }

            return DTSValidationStatus.VS_ISVALID;
        }

        private void SetComponentVersion()
        {
            // Get the assembly version and set that as our current version.
            DtsPipelineComponentAttribute attr = (DtsPipelineComponentAttribute)
                    Attribute.GetCustomAttribute(this.GetType(), typeof(DtsPipelineComponentAttribute), false);

            ComponentMetaData.Version = attr.CurrentVersion;
        }

        public override void PerformUpgrade(int pipelineVersion)
        {
            // Get the assembly version and set that as our current version.
            SetComponentVersion();
        }
    }
}
