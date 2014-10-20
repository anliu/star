using Microsoft.HBase.Client.Common;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
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
        private Dictionary<string, PipelineColumnInfo> _bufferColumnInfo;
        private HBaseClient _hbaseClient;
        internal bool Connected { get; set; }

        public override void ProvideComponentProperties()
        {
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

            // Add the batch size property.
            var propBatchSize = ComponentMetaData.CustomPropertyCollection.New();
            propBatchSize.Name = Constants.PropBatchSize;
            propBatchSize.Description = "Batch size (in kilobyte)";
            propBatchSize.Value = 1024;

            IDTSInput100 input = ComponentMetaData.InputCollection.New();
            input.Name = "Input";
            input.HasSideEffects = true;
            input.ExternalMetadataColumnCollection.IsUsed = true;

            // add error row disposition, default is to fail component.
            input.ErrorRowDisposition = DTSRowDisposition.RD_FailComponent;

            // Get the assembly version and set that as our current version.
            SetComponentVersion();

            // Insert an error output.
            var errorOutput = ComponentMetaData.OutputCollection.New();
            errorOutput.Name = "ErrorOutput";
            errorOutput.IsErrorOut = true;
            errorOutput.SynchronousInputID = input.ID;
            errorOutput.ExclusionGroup = 1;

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

        public override IDTSInputColumn100 SetUsageType(int inputID, IDTSVirtualInput100 virtualInput, int lineageID, DTSUsageType usageType)
        {
            if (usageType == DTSUsageType.UT_READWRITE)
            {
                bool bCancel;
                ErrorSupport.FireErrorWithArgs(
                    HResults.DTS_E_CANTSETUSAGETYPETOREADWRITE,
                    out bCancel, virtualInput.IdentificationString, lineageID);
                throw new PipelineComponentHResultException(HResults.DTS_E_CANTSETUSAGETYPETOREADWRITE);
            }
            else
            {
                return base.SetUsageType(inputID, virtualInput, lineageID, usageType);
            }
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
            var outputError = ComponentMetaData.OutputCollection[iErrorOutIndex];
            var inputMain = ComponentMetaData.InputCollection[0];

            // start fresh
            inputMain.InputColumnCollection.RemoveAll();
            inputMain.ExternalMetadataColumnCollection.RemoveAll();
            for (i = outputError.OutputColumnCollection.Count - 1; i >= 0; i--)
            {
                // remove non special error columns, scan backward as item index
                // can change after item removal
                if (outputError.OutputColumnCollection[i].SpecialFlags == 0)
                {
                    outputError.OutputColumnCollection.RemoveObjectByIndex(i);
                }
            }

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
            for (i = 0; i < columnList.Length; i++)
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
                var outputcolNewError = outputError.OutputColumnCollection.NewAt(i);
                var outputcolNewExternal = inputMain.ExternalMetadataColumnCollection.NewAt(i);

                outputcolNewError.Name = columnName;
                outputcolNewExternal.Name = columnName;

                // set the external metadata column properties
                outputcolNewExternal.DataType = dtstype;
                outputcolNewExternal.Length = Length;
                outputcolNewExternal.Precision = Precision;
                outputcolNewExternal.Scale = Scale;
                outputcolNewExternal.CodePage = CodePage;

                // set the output column properties
                outputcolNewError.SetDataTypeProperties(dtstype, Length, Precision, Scale, CodePage);
            }
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
                var input = ComponentMetaData.InputCollection[0];
                var inputCols = input.InputColumnCollection;
                var externCols = input.ExternalMetadataColumnCollection;

                for (int i = 0; i < inputCols.Count; i++)
                {
                    var inputCol = inputCols[i];

                    // hbase doesn't have type system, convert to string type or
                    // image before send to hbase dest
                    if (inputCol.DataType != DataType.DT_STR &&
                        inputCol.DataType != DataType.DT_WSTR &&
                        inputCol.DataType != DataType.DT_IMAGE)
                    {
                        bool bCancel;
                        ErrorSupport.FireErrorWithArgs(
                            HResults.DTS_E_INVALIDDATATYPE,
                            out bCancel, inputCol.IdentificationString, inputCol.DataType);
                        return DTSValidationStatus.VS_ISBROKEN;
                    }

                    try
                    {
                        // check the external meta data column is valid, which
                        // will ensure the execute phase we have a valid column
                        // qualifier for hbase
                        externCols.FindObjectByID(inputCol.ExternalMetadataColumnID);
                    }
                    catch (COMException)
                    {
                        // There is no external metadata column for this input column.
                        bool bCancel;
                        ErrorSupport.FireErrorWithArgs(
                            HResults.DTS_E_COLUMNMAPPEDTONONEXISTENTEXTERNALMETADATACOLUMN,
                            out bCancel, inputCol.IdentificationString);
                        return DTSValidationStatus.VS_ISBROKEN;
                    }
                }

                if (inputCols.Count == 0 || externCols.Count == 0)
                {
                    bool bCancel;
                    ErrorSupport.FireErrorWithArgs(
                        HResults.DTS_E_CANNOTHAVEZEROINPUTCOLUMNS,
                                out bCancel, input.IdentificationString);
                    return DTSValidationStatus.VS_ISBROKEN;
                }

                // validate table existence, columns are always available per
                // hbase dynamic column model
                try
                {
                    this._hbaseClient.GetTableInfo(propTableName.Value.ToString());
                }
                catch (AggregateException)
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

            // get error output
            int iErrorOutID = 0, iErrorOutIndex = 0;
            GetErrorOutputInfo(ref iErrorOutID, ref iErrorOutIndex);
            var outputError = ComponentMetaData.OutputCollection[iErrorOutIndex];
            var inputMain = ComponentMetaData.InputCollection[0];
            var externCols = inputMain.ExternalMetadataColumnCollection;

            if (!this.Connected)
            {
                bool bCancel;
                ErrorSupport.FireError(HResults.DTS_E_CONNECTIONREQUIREDFORREAD, out bCancel);
                throw new PipelineComponentHResultException(HResults.DTS_E_CONNECTIONREQUIREDFORREAD);
            }

            // in case outputMain is null, let it throw, just like an assertion
            this._bufferColumnInfo = new Dictionary<string, PipelineColumnInfo>(inputMain.InputColumnCollection.Count);

            // buffer layout is only fixed during PreExecute phase, keep a copy
            // of the buffer column index so that we can set data in PrimeOutput
            for (var i = 0; i < inputMain.InputColumnCollection.Count; i++)
            {
                var col = inputMain.InputColumnCollection[i];
                var ext = externCols.GetObjectByID(col.ExternalMetadataColumnID);

                this._bufferColumnInfo[ext.Name] = new PipelineColumnInfo()
                {
                    BufferColumnIndex = BufferManager.FindColumnByLineageID(inputMain.Buffer, col.LineageID),
                    InOutColumnIndex = i
                };
            }
        }

        public override void ProcessInput(int inputID, PipelineBuffer buffer)
        {
            var input = ComponentMetaData.InputCollection[0];
            var keyColumn = input.ExternalMetadataColumnCollection[0].Name;
            var set = new CellSet();
            var currentSize = 0;
            // get table name
            var propTableName = ComponentMetaData.CustomPropertyCollection[Constants.PropTableName];

            // get batch size
            var propBatchSize = ComponentMetaData.CustomPropertyCollection[Constants.PropBatchSize];
            var batchSize = 1024 * 1024;

            if (propBatchSize != null && (int)propBatchSize.Value > 0){
                batchSize = (int)propBatchSize.Value * 1024;
            }

            while (buffer.NextRow())
            {
                var row = new CellSet.Row();
                var rowSize = 0;

                foreach (var col in this._bufferColumnInfo)
                {
                    if (col.Key == keyColumn)
                    {
                        if (buffer.IsNull(col.Value.BufferColumnIndex))
                        {
                            break;
                        }

                        row.key = GetBufferColumn(buffer, col.Value);
                        rowSize += row.key.Length;
                    }
                    else if (!buffer.IsNull(col.Value.BufferColumnIndex))
                    {
                        var value = new Cell
                        {
                            column = Encoding.UTF8.GetBytes(col.Key),
                            data = GetBufferColumn(buffer, col.Value)
                        };

                        row.values.Add(value);
                        rowSize += (value.column.Length + value.data.Length);
                    }
                }

                if (row.key != null && row.key.Length > 0)
                {
                    // valid row, count the size
                    set.rows.Add(row);
                    currentSize += rowSize;
                }
                else if (input.ErrorRowDisposition == DTSRowDisposition.RD_RedirectRow)
                {
                    // redirect rows
                    var inputColIndex = this._bufferColumnInfo[keyColumn].InOutColumnIndex;

                    buffer.DirectErrorRow(ComponentMetaData.OutputCollection[0].ID,
                            HResults.DTS_E_NOKEYCOLS,
                            input.InputColumnCollection[inputColIndex].LineageID);
                }
                else if (input.ErrorRowDisposition == DTSRowDisposition.RD_FailComponent)
                {
                    // throw error
                    bool bCancel;
                    ErrorSupport.FireErrorWithArgs(HResults.DTS_E_NOKEYCOLS,
                            out bCancel,
                            input.IdentificationString);

                    throw new PipelineComponentHResultException(HResults.DTS_E_NOKEYCOLS);
                }

                if (currentSize > batchSize)
                {
                    // send the batch
                    this._hbaseClient.StoreCells(propTableName.Value.ToString(), set);

                    // reset
                    currentSize = 0;
                    set = new CellSet();
                }
            }

            // send in case there are any rows left
            if (set.rows.Count > 0)
            {
                this._hbaseClient.StoreCells(propTableName.Value.ToString(), set);
            }
        }

        private byte[] GetBufferColumn(PipelineBuffer buffer, PipelineColumnInfo ci)
        {
            var col = ci.BufferColumnIndex;
            var colInfo = buffer.GetColumnInfo(col);

            if (colInfo.DataType == DataType.DT_IMAGE)
            {
                // may need to convert from UTF8 to UTF16 if we support DT_NTEXT
                // which might double the memory footprint so that we favor of
                // DT_IMAGE for now
                return buffer.GetBlobData(col, 0, (int)buffer.GetBlobLength(col));
            }
            else if (colInfo.DataType == DataType.DT_STR || colInfo.DataType == DataType.DT_WSTR)
            {
                // use UTF8 for now, may need to expose an option later
                return Encoding.UTF8.GetBytes(buffer.GetString(col));
            }

            return null;
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
