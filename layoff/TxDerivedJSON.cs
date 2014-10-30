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
            DisplayName = "Derived JSON",
            Description = "Derive JSON objects from columns",
            RequiredProductLevel = Microsoft.SqlServer.Dts.Runtime.Wrapper.DTSProductLevel.DTSPL_NONE,
            UITypeName = "Microsoft.HBase.Client.UI.TxJSONDerivedUI, Microsoft.HBase.Client.DtsComponents")
    ]
    public class TxDerivedJSON : PipelineComponent
    {
        private Dictionary<string, PipelineColumnInfo> mappingPaths;
        private PipelineColumnInfo outputColInfo;

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
            input.Name = "Input";
            input.ExternalMetadataColumnCollection.IsUsed = true;

            IDTSOutput100 output = ComponentMetaData.OutputCollection.New();
            output.Name = "JSON output";
            output.SynchronousInputID = input.ID;
            output.ExternalMetadataColumnCollection.IsUsed = false;

            var outputCol = output.OutputColumnCollection.New();
            outputCol.Name = "JSON";
            outputCol.SetDataTypeProperties(DataType.DT_IMAGE, 0, 0, 0, 0);

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

        public override void ReinitializeMetaData()
        {
            // baseclass may have some work to do here
            base.ReinitializeMetaData();

            // which output is the error output?
            int i, iErrorOutID = 0, iErrorOutIndex = 0;
            GetErrorOutputInfo(ref iErrorOutID, ref iErrorOutIndex);

            // get the output which is not the error output
            var outputMain = ComponentMetaData.OutputCollection[iErrorOutIndex == 0 ? 1 : 0];
            var outputError = ComponentMetaData.OutputCollection[iErrorOutIndex];

            // start fresh
            outputMain.OutputColumnCollection.RemoveAll();
            var outputCol = outputMain.OutputColumnCollection.New();
            outputCol.Name = "JSON";
            outputCol.SetDataTypeProperties(DataType.DT_IMAGE, 0, 0, 0, 0);

            for (i = outputError.OutputColumnCollection.Count - 1; i >= 0; i--)
            {
                // remove non special error columns, scan backward as item index
                // can change after item removal
                if (outputError.OutputColumnCollection[i].SpecialFlags == 0)
                {
                    outputError.OutputColumnCollection.RemoveObjectByIndex(i);
                }
            }

            var inputMain = ComponentMetaData.InputCollection[0];

            // start fresh
            inputMain.InputColumnCollection.RemoveAll();
            inputMain.ExternalMetadataColumnCollection.RemoveAll();

            var propMapping = ComponentMetaData.CustomPropertyCollection[Constants.PropMapping];
            if (!string.IsNullOrEmpty((string)propMapping.Value))
            {
                i = 0;
                foreach (var prop in JsonParser.GetPropertyList(propMapping.Value.ToString()))
                {
                    var externCol = inputMain.ExternalMetadataColumnCollection.NewAt(i++);
                    externCol.Name = prop;
                    externCol.DataType = DataType.DT_WSTR;
                    externCol.Length = 50;
                    externCol.Precision = 0;
                    externCol.Scale = 0;
                    externCol.CodePage = 0;
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

            int iErrorOutID = 0, iErrorOutIndex = 0;
            GetErrorOutputInfo(ref iErrorOutID, ref iErrorOutIndex);
            var outputMain = ComponentMetaData.OutputCollection[iErrorOutIndex == 0 ? 1 : 0];

            // should have one output, one column (except error output)
            if (ComponentMetaData.OutputCollection.Count > 2 ||
                outputMain.OutputColumnCollection.Count != 1)
            {
                return DTSValidationStatus.VS_ISCORRUPT;
            }

            // should have one inputs
            if (ComponentMetaData.InputCollection.Count != 1)
            {
                return DTSValidationStatus.VS_ISCORRUPT;
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
            var inputMain = ComponentMetaData.InputCollection[0];
            var externCols = inputMain.ExternalMetadataColumnCollection;

            // in case outputMain is null, let it throw, just like an assertion
            this.mappingPaths = new Dictionary<string, PipelineColumnInfo>(inputMain.InputColumnCollection.Count);

            // buffer layout is only fixed during PreExecute phase, keep a copy
            // of the buffer column index so that we can set data in PrimeOutput
            for (var i = 0; i < inputMain.InputColumnCollection.Count; i++)
            {
                var col = inputMain.InputColumnCollection[i];
                var ext = externCols.GetObjectByID(col.ExternalMetadataColumnID);

                this.mappingPaths[ext.Name] = new PipelineColumnInfo()
                {
                    BufferColumnIndex = BufferManager.FindColumnByLineageID(inputMain.Buffer, col.LineageID),
                    InOutColumnIndex = i
                };
            }

            // output should be only one column
            this.outputColInfo = new PipelineColumnInfo
            {
                BufferColumnIndex = BufferManager.FindColumnByLineageID(inputMain.Buffer, outputMain.OutputColumnCollection[0].LineageID),
                InOutColumnIndex = 0
            };
        }

        public override void ProcessInput(int inputID, PipelineBuffer buffer)
        {
            while (buffer.NextRow())
            {
                var col = this.outputColInfo.BufferColumnIndex;
                var outputType = buffer.GetColumnInfo(col).DataType;
                var propMapping = ComponentMetaData.CustomPropertyCollection[Constants.PropMapping];
                // build the output object structure so that it's easier to set
                // values (it's much easier to SelectToken than CreateToken with
                // path in JSON.net)
                var obj = JObject.Parse(propMapping.Value.ToString());

                foreach (var mapping in this.mappingPaths)
                {
                    var inputType = buffer.GetColumnInfo(mapping.Value.BufferColumnIndex).DataType;
                    string propValue = string.Empty;

                    var token = obj.SelectToken(mapping.Key, false);
                    if (token == null)
                    {
                        // error out
                        continue;
                    }

                    if (buffer.IsNull(mapping.Value.BufferColumnIndex))
                    {
                        // set null value as every object is created via mapping
                        // json string, the value is preset to whatever value in
                        // the mapping property value
                        token.Replace(JValue.CreateNull());
                        continue;
                    }

                    if (inputType == DataType.DT_STR || inputType == DataType.DT_WSTR)
                    {
                        propValue = buffer.GetString(mapping.Value.BufferColumnIndex);
                    }
                    else if (inputType == DataType.DT_IMAGE)
                    {
                        var len = (int)buffer.GetBlobLength(mapping.Value.BufferColumnIndex);
                        propValue = Encoding.UTF8.GetString(buffer.GetBlobData(mapping.Value.BufferColumnIndex, 0, len));
                    }

                    if (token.Type == JTokenType.String)
                    {
                        token.Replace(JValue.CreateString(propValue));
                    }
                    else if (token.Type == JTokenType.Object)
                    {
                        token.Replace(JObject.Parse(propValue));
                    }
                    else if (token.Type == JTokenType.Array)
                    {
                        token.Replace(JArray.Parse(propValue));
                    }
                    // todo check column type instead nad handle other types
                }

                if (outputType == DataType.DT_IMAGE)
                {
                    buffer.AddBlobData(this.outputColInfo.BufferColumnIndex, Encoding.UTF8.GetBytes(obj.ToString()));
                }
                else if (outputType == DataType.DT_STR || outputType == DataType.DT_WSTR)
                {
                    buffer.SetString(this.outputColInfo.BufferColumnIndex, obj.ToString());
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
