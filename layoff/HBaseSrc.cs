using Microsoft.HBase.Client.Common;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.HBase.Client
{
    [
        DtsPipelineComponent(ComponentType = ComponentType.SourceAdapter,
            CurrentVersion = 4,
            DisplayName = "HBase Source",
            Description = "HBase Source Component",
            RequiredProductLevel = Microsoft.SqlServer.Dts.Runtime.Wrapper.DTSProductLevel.DTSPL_NONE)
    ]
    public class HBaseSrc : PipelineComponent
    {
        private Dictionary<string, PipelineColumnInfo> _bufferColumnInfo;
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
            var propColumns= ComponentMetaData.CustomPropertyCollection.New();
            propColumns.Name = Constants.PropColumns;
            propColumns.Description = "Columns to retrieve";
            propColumns.Value = string.Empty;

            // Add the columns property.
            var propColumnTypes = ComponentMetaData.CustomPropertyCollection.New();
            propColumnTypes.Name = Constants.PropColumnTypes;
            propColumnTypes.Description = "Type of columns to retrieve";
            propColumnTypes.Value = string.Empty;

            IDTSOutput100 output = ComponentMetaData.OutputCollection.New();
            output.Name = "Output";

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

                this._hbaseClient = cmhbase.AcquireConnection(transaction) as HBaseClient;
                this.Connected = true;
            }
        }

        public override void ReleaseConnections()
        {
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

            string columnTypesValue = string.Empty;

            // which output is the error output?
            int i, iErrorOutID = 0, iErrorOutIndex = 0;
            GetErrorOutputInfo(ref iErrorOutID, ref iErrorOutIndex);

            // get the output which is not the error output
            var outputMain = ComponentMetaData.OutputCollection[iErrorOutIndex == 0 ? 1 : 0];
            var outputError = ComponentMetaData.OutputCollection[iErrorOutIndex];

            // start fresh
            outputMain.OutputColumnCollection.RemoveAll();
            outputMain.ExternalMetadataColumnCollection.RemoveAll();
            for (i = outputError.OutputColumnCollection.Count - 1; i >= 0; i--)
            {
                // remove non special error columns, scan backward as item index
                // can change after item removal
                if (outputError.OutputColumnCollection[i].SpecialFlags == 0)
                {
                    outputError.OutputColumnCollection.RemoveObjectByIndex(i);
                }
            }

            outputMain.ExclusionGroup = 0;
            outputMain.SynchronousInputID = 0;

            // try to read column types property
            var propColumnTypes = ComponentMetaData.CustomPropertyCollection[Constants.PropColumnTypes];
            if (propColumnTypes != null && propColumnTypes.Value != null &&
                !string.IsNullOrEmpty(propColumnTypes.Value.ToString().Trim()))
            {
                columnTypesValue = propColumnTypes.Value.ToString();
            }

            var columnTypeList = columnTypesValue.Split('|');

            // process columns property
            var propColumns = ComponentMetaData.CustomPropertyCollection[Constants.PropColumns];
            if (propColumns == null || propColumns.Value == null ||
                string.IsNullOrEmpty(propColumns.Value.ToString().Trim()))
            {
                return;
            }

            var columnList = propColumns.Value.ToString().Split('|');
            for (i = 0; i < columnList.Length; i ++)
            {
                int Length = 0;
                int Precision = 0;
                int Scale = 0;
                int CodePage = 0;

                var columnQualifier = columnList[i];
                var columnType = columnTypeList.Length > i ? columnTypeList[i] : "0";
                int.TryParse(columnType, out Length);

                var dtstype = Length != 0 ? DataType.DT_WSTR : DataType.DT_IMAGE;

                var columnName = columnList[i];

                // check the name
                if (string.IsNullOrEmpty(columnName))
                {
                    bool bCancel;
                    ErrorSupport.FireError(HResults.DTS_E_DATASOURCECOLUMNWITHNONAMEFOUND, out bCancel);
                    throw new PipelineComponentHResultException(HResults.DTS_E_DATASOURCECOLUMNWITHNONAMEFOUND);
                }

                // create a new column
                var outputcolNewMain = outputMain.OutputColumnCollection.NewAt(i);
                var outputcolNewError = outputError.OutputColumnCollection.NewAt(i);
                var outputcolNewExternal = outputMain.ExternalMetadataColumnCollection.NewAt(i);

                outputcolNewMain.Name = columnName;
                outputcolNewError.Name = columnName;
                outputcolNewExternal.Name = columnName;

                // set the external metadata column properties
                outputcolNewExternal.DataType = dtstype;
                outputcolNewExternal.Length = Length;
                outputcolNewExternal.Precision = Precision;
                outputcolNewExternal.Scale = Scale;
                outputcolNewExternal.CodePage = CodePage;

                // set the output column properties
                outputcolNewMain.SetDataTypeProperties(dtstype, Length, Precision, Scale, CodePage);
                outputcolNewError.SetDataTypeProperties(dtstype, Length, Precision, Scale, CodePage);

                // wire the output column to the external metadata
                outputcolNewMain.ExternalMetadataColumnID = outputcolNewExternal.ID;

                // set the default error dispositions
                outputcolNewMain.ErrorRowDisposition = DTSRowDisposition.RD_FailComponent;
                outputcolNewMain.TruncationRowDisposition = DTSRowDisposition.RD_FailComponent;
                outputcolNewMain.ErrorOrTruncationOperation = "ErrorOrTruncationOperationConversion";
            }
        }

        public override DTSValidationStatus Validate()
        {
            var status = base.Validate();
            if (status == DTSValidationStatus.VS_ISCORRUPT)
            {
                return status;
            }

            // should have no input
            if (ComponentMetaData.InputCollection.Count != 0)
            {
                return DTSValidationStatus.VS_ISCORRUPT;
            }

            // should have two outputs
            if (ComponentMetaData.OutputCollection.Count > 2)
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

            var propColumns= ComponentMetaData.CustomPropertyCollection[Constants.PropColumns];
            if (propColumns == null || propColumns.Value == null ||
                string.IsNullOrEmpty(propColumns.Value.ToString().Trim()))
            {
                return DTSValidationStatus.VS_ISCORRUPT;
            }

            if (this.Connected && ComponentMetaData.ValidateExternalMetadata == true)
            {
                // validate table existence, columns are always available per
                // hbase dynamic column model
                try
                {
                    this._hbaseClient.GetTableInfo(propTableName.Value.ToString());
                }
                catch(AggregateException)
                {
                    bool bCancel;
                    ErrorSupport.FireErrorWithArgs(HResults.DTS_E_INCORRECTCUSTOMPROPERTYVALUEFOROBJECT,
                        out bCancel, Constants.PropTableName, ComponentMetaData.IdentificationString);
                    return DTSValidationStatus.VS_ISBROKEN;
                }
                catch (WebException)
                {
                    bool bCancel;
                    ErrorSupport.FireErrorWithArgs(HResults.DTS_E_INCORRECTCUSTOMPROPERTYVALUEFOROBJECT,
                        out bCancel, Constants.PropTableName, ComponentMetaData.IdentificationString);
                    return DTSValidationStatus.VS_ISBROKEN;
                }
            }

            return DTSValidationStatus.VS_ISVALID;
        }

        public override void PreExecute()
        {
            // baseclass may need to do some work
            base.PreExecute();

            //get the non-error output
            int iErrorOutID = 0, iErrorOutIndex = 0;
            GetErrorOutputInfo(ref iErrorOutID, ref iErrorOutIndex);
            var outputMain = ComponentMetaData.OutputCollection[iErrorOutIndex == 0 ? 1 : 0];

            if (!this.Connected)
            {
                bool bCancel;
                ErrorSupport.FireError(HResults.DTS_E_CONNECTIONREQUIREDFORREAD, out bCancel);
                throw new PipelineComponentHResultException(HResults.DTS_E_CONNECTIONREQUIREDFORREAD);
            }

            // in case outputMain is null, let it throw, just like an assertion
            this._bufferColumnInfo = new Dictionary<string, PipelineColumnInfo>(outputMain.OutputColumnCollection.Count);

            // buffer layout is only fixed during PreExecute phase, keep a copy
            // of the buffer column index so that we can set data in PrimeOutput
            for (var i = 0; i < outputMain.OutputColumnCollection.Count; i++)
            {
                var col = outputMain.OutputColumnCollection[i];

                this._bufferColumnInfo[col.Name] = new PipelineColumnInfo()
                {
                    BufferColumnIndex = BufferManager.FindColumnByLineageID(outputMain.Buffer, col.LineageID),
                    InOutColumnIndex = i
                };
            }
        }

        public override void PrimeOutput(int outputs, int[] outputIDs, PipelineBuffer[] buffers)
        {
            // get output buffers
            PipelineBuffer bufferMain = buffers[0], bufferError = null;

            // If there is an error output, figure out which output is the main
            // and which is the error
            if (outputs == 2)
            {
                int iErrorOutID = 0, iErrorOutIndex = 0;
                GetErrorOutputInfo(ref iErrorOutID, ref iErrorOutIndex);

                if (outputIDs[0] == iErrorOutID)
                {
                    bufferMain = buffers[1];
                    bufferError = buffers[0];
                }
                else
                {
                    bufferMain = buffers[0];
                    bufferError = buffers[1];
                }
            }

            // get table name
            var propTableName = ComponentMetaData.CustomPropertyCollection[Constants.PropTableName];

            // create scanner
            var scanSettings = new Scanner()
            {
                batch = 1024
            };

            var scannerInfo = this._hbaseClient.CreateScanner(propTableName.Value.ToString(), scanSettings);
            CellSet next = null;
            while ((next = this._hbaseClient.ScannerGetNext(scannerInfo)) != null)
            {
                foreach (var row in next.rows)
                {
                    // may enter wait
                    bufferMain.AddRow();

                    // copy row to the buffer
                    if (this._bufferColumnInfo.ContainsKey(":key"))
                    {
                        SetBufferColumn(bufferMain, this._bufferColumnInfo[":key"].BufferColumnIndex, row.key);
                    }

                    // mapping via column name, which is slow but there seems no
                    // better ways due to the dynamic column model of hbase which
                    // is core advantage of hbase
                    // the best we can have might be some kind of merge between
                    // two column list but it seems not much helpful as we have
                    // a column dictionary built for free already.
                    for (var i = 0; i < row.values.Count; i++)
                    {
                        // UTF8 seems working so far, use base64 for arbitrary data
                        // if necessary
                        var colName = Encoding.UTF8.GetString(row.values[i].column);
                        if (!this._bufferColumnInfo.ContainsKey(colName))
                        {
                            continue;
                        }

                        var colIndex = this._bufferColumnInfo[colName].BufferColumnIndex;
                        SetBufferColumn(bufferMain, colIndex, row.values[i].data);
                    }
                }
            }

            // done
            bufferMain.SetEndOfRowset();
        }

        private void SetBufferColumn(PipelineBuffer buffer, int col, byte[] data)
        {
            var colInfo = buffer.GetColumnInfo(col);

            if (colInfo.DataType == DataType.DT_IMAGE)
            {
                // may need to convert from UTF8 to UTF16 if we use
                // DT_NTEXT which might double the memory footprint
                // so that we favor of DT_IMAGE for now, try json/xml
                // to see if it's already done in the client lib
                buffer.AddBlobData(col, data);
            }
            else
            {
                // UTF8 seems working so far, use base64 for arbitrary data
                // if needed
                var stringValue = Encoding.UTF8.GetString(data);
                var subLength = colInfo.MaxLength;

                // todo handle redirection for truncation
                if (stringValue.Length < colInfo.MaxLength)
                {
                    subLength = stringValue.Length;
                }

                buffer.SetString(col, stringValue.Substring(0, subLength));
            }
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
