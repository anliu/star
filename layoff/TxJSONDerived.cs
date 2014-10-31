using Microsoft.HBase.Client.Common;
using Microsoft.HBase.Client.Utilities;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.HBase.Client
{
    [
        DtsPipelineComponent(ComponentType = ComponentType.Transform,
            CurrentVersion = 4,
            DisplayName = "JSON Derived Columns",
            Description = "Derive columns from JSON objects",
            RequiredProductLevel = Microsoft.SqlServer.Dts.Runtime.Wrapper.DTSProductLevel.DTSPL_NONE,
            UITypeName = "Microsoft.HBase.Client.UI.TxJSONDerivedUI, Microsoft.HBase.Client.DtsComponentUI")
    ]
    public class TxJSONDerived : PipelineComponent
    {
        private Dictionary<string, PipelineColumnInfo> mappingPaths;
        private PipelineColumnInfo inputColInfo;

        public override void ProvideComponentProperties()
        {
            // start out clean, remove anything put on by the base class
            RemoveAllInputsOutputsAndCustomProperties();

            // error dispositions
            ComponentMetaData.UsesDispositions = true;

            // Add the mapping property.
            var propMapping = ComponentMetaData.CustomPropertyCollection.New();
            propMapping.Name = Constants.PropMapping;
            propMapping.Description = "Mapping (in json format)";
            propMapping.Value = string.Empty;

            IDTSInput100 input = ComponentMetaData.InputCollection.New();
            input.Name = "JSON input";

            IDTSOutput100 output = ComponentMetaData.OutputCollection.New();
            output.Name = "Output";
            output.SynchronousInputID = input.ID;
            output.ExternalMetadataColumnCollection.IsUsed = false;

            // Get the assembly version and set that as our current version.
            SetComponentVersion();

            // Insert an error output.
            var errorOutput = ComponentMetaData.OutputCollection.New();
            errorOutput.Name = "ErrorOutput";
            errorOutput.IsErrorOut = true;
            errorOutput.SynchronousInputID = input.ID;
            errorOutput.ExclusionGroup = 1;
        }

        //public override IDTSCustomProperty100 SetComponentProperty(string propertyName, object propertyValue)
        //{
        //    if (propertyName == propMappingName)
        //    {
        //        this.mappingPaths = JsonParser.GetPropertyList((string)propertyValue);
        //    }

        //    return base.SetComponentProperty(propertyName, propertyValue);
        //}

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

        public override void SetOutputColumnDataTypeProperties(int iOutputID, int iOutputColumnID, DataType eDataType, int iLength, int iPrecision, int iScale, int iCodePage)
        {
            if (eDataType != DataType.DT_WSTR && eDataType != DataType.DT_STR &&
                eDataType != DataType.DT_IMAGE && eDataType != DataType.DT_NTEXT && eDataType != DataType.DT_TEXT)
            {
                throw new PipelineComponentHResultException(HResults.DTS_E_UNEXPECTEDCOLUMNDATATYPE);
            }

            var output = ComponentMetaData.OutputCollection.FindObjectByID(iOutputID);
            var col = output.OutputColumnCollection.FindObjectByID(iOutputColumnID);
            col.SetDataTypeProperties(eDataType, iLength, iPrecision, iScale, iCodePage);
        }

        public override void ReinitializeMetaData()
        {
            // baseclass may have some work to do here
            base.ReinitializeMetaData();

            int iErrorOutID = 0, iErrorOutIndex = 0;
            GetErrorOutputInfo(ref iErrorOutID, ref iErrorOutIndex);
            var outputMain = ComponentMetaData.OutputCollection[iErrorOutIndex == 0 ? 1 : 0];
            var inputMain = ComponentMetaData.InputCollection[0];

            // start fresh
            inputMain.InputColumnCollection.RemoveAll();

            var propMapping = ComponentMetaData.CustomPropertyCollection[Constants.PropMapping];
            if (propMapping != null && !string.IsNullOrEmpty((string)propMapping.Value))
            {
                int i = 0;
                // clear the current output columns
                outputMain.OutputColumnCollection.RemoveAll();

                foreach (var prop in JsonParser.GetPropertyList((string)propMapping.Value))
                {
                    var outputCol = outputMain.OutputColumnCollection.NewAt(i++);
                    outputCol.Name = prop;
                    outputCol.SetDataTypeProperties(DataType.DT_WSTR, 50, 0, 0, 0);
                }
            }
        }

        public override DTSValidationStatus Validate()
        {
            var status = base.Validate();
            if (status == DTSValidationStatus.VS_ISCORRUPT)
            {
                return status;
            }

            // should have one input, one column
            if (ComponentMetaData.InputCollection.Count != 1 ||
                ComponentMetaData.InputCollection[0].InputColumnCollection.Count != 1)
            {
                return DTSValidationStatus.VS_ISCORRUPT;
            }

            // should have two outputs
            if (ComponentMetaData.OutputCollection.Count > 2)
            {
                return DTSValidationStatus.VS_ISCORRUPT;
            }

            var inputMain = ComponentMetaData.InputCollection[0];
            var eDataType = inputMain.InputColumnCollection[0].DataType;

            if (eDataType != DataType.DT_WSTR && eDataType != DataType.DT_STR &&
                eDataType != DataType.DT_IMAGE && eDataType != DataType.DT_NTEXT && eDataType != DataType.DT_TEXT)
            {
                throw new PipelineComponentHResultException(HResults.DTS_E_UNEXPECTEDCOLUMNDATATYPE);
            }

            int iErrorOutID = 0, iErrorOutIndex = 0;
            GetErrorOutputInfo(ref iErrorOutID, ref iErrorOutIndex);
            var outputMain = ComponentMetaData.OutputCollection[iErrorOutIndex == 0 ? 1 : 0];

            for (var i = 0; i < outputMain.OutputColumnCollection.Count; i++)
            {
                var col = outputMain.OutputColumnCollection[i];
                eDataType = col.DataType;
                if (eDataType != DataType.DT_WSTR && eDataType != DataType.DT_STR &&
                    eDataType != DataType.DT_IMAGE && eDataType != DataType.DT_NTEXT && eDataType != DataType.DT_TEXT)
                {
                    throw new PipelineComponentHResultException(HResults.DTS_E_UNEXPECTEDCOLUMNDATATYPE);
                }
            }

            return DTSValidationStatus.VS_ISVALID;
        }

        public override void PreExecute()
        {
            // baseclass may need to do some work
            base.PreExecute();

            int iErrorOutID = 0, iErrorOutIndex = 0;
            GetErrorOutputInfo(ref iErrorOutID, ref iErrorOutIndex);
            var outputMain = ComponentMetaData.OutputCollection[iErrorOutIndex == 0 ? 1 : 0];
            // input should be only one column
            var inputMain = ComponentMetaData.InputCollection[0];

            // in case outputMain is null, let it throw, just like an assertion
            this.mappingPaths = new Dictionary<string, PipelineColumnInfo>(outputMain.OutputColumnCollection.Count);

            // buffer layout is only fixed during PreExecute phase, keep a copy
            // of the buffer column index
            // it's a synchronous transform so that always look up via the input
            // buffer
            for (var i = 0; i < outputMain.OutputColumnCollection.Count; i++)
            {
                var col = outputMain.OutputColumnCollection[i];

                this.mappingPaths[col.Name] = new PipelineColumnInfo()
                {
                    BufferColumnIndex = BufferManager.FindColumnByLineageID(inputMain.Buffer, col.LineageID),
                    InOutColumnIndex = i
                };
            }

            this.inputColInfo = new PipelineColumnInfo
            {
                BufferColumnIndex = BufferManager.FindColumnByLineageID(inputMain.Buffer, inputMain.InputColumnCollection[0].LineageID),
                InOutColumnIndex = 0
            };
        }

        public override void ProcessInput(int inputID, PipelineBuffer buffer)
        {
            while (buffer.NextRow())
            {
                var col = this.inputColInfo.BufferColumnIndex;
                var inputType = buffer.GetColumnInfo(col).DataType;
                string inputValue = string.Empty;
                if (inputType == DataType.DT_IMAGE)
                {
                    inputValue = Encoding.UTF8.GetString(buffer.GetBlobData(col, 0, (int)buffer.GetBlobLength(col)));
                }
                else if (inputType == DataType.DT_STR || inputType == DataType.DT_WSTR ||
                    inputType == DataType.DT_TEXT || inputType == DataType.DT_NTEXT)
                {
                    inputValue = buffer.GetString(col);
                }

                JObject obj = JObject.Parse(inputValue);
                foreach (var mapping in this.mappingPaths)
                {
                    var token = obj.SelectToken(mapping.Key, false);
                    if (token == null)
                    {
                        continue;
                    }

                    var outputType = buffer.GetColumnInfo(mapping.Value.BufferColumnIndex).DataType;
                    if (outputType == DataType.DT_IMAGE)
                    {
                        buffer.AddBlobData(mapping.Value.BufferColumnIndex, Encoding.UTF8.GetBytes(token.ToString()));
                    }
                    else if (outputType == DataType.DT_STR || outputType == DataType.DT_WSTR ||
                        outputType == DataType.DT_TEXT || outputType == DataType.DT_NTEXT)
                    {
                        buffer.SetString(mapping.Value.BufferColumnIndex, token.ToString());
                    }
                }
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
