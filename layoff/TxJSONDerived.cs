using Microsoft.HBase.Client.Utilities;
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
        DtsPipelineComponent(ComponentType = ComponentType.Transform,
            CurrentVersion = 4,
            DisplayName = "JSON Derived Columns Component",
            Description = "Derive columns from JSON objects",
            RequiredProductLevel = Microsoft.SqlServer.Dts.Runtime.Wrapper.DTSProductLevel.DTSPL_NONE)
    ]
    public class TxJSONDerived : PipelineComponent
    {
        private const string propMappingName = "Mapping";
        private IEnumerable<string> mappingPaths;

        public override void ProvideComponentProperties()
        {
            // start out clean, remove anything put on by the base class
            RemoveAllInputsOutputsAndCustomProperties();

            // error dispositions
            ComponentMetaData.UsesDispositions = true;

            // Add the table name property.
            var propMapping = ComponentMetaData.CustomPropertyCollection.New();
            propMapping.Name = propMappingName;
            propMapping.Description = "Mapping (in json format)";
            propMapping.Value = string.Empty;

            IDTSInput100 input = ComponentMetaData.InputCollection.New();
            input.Name = "Input";

            IDTSOutput100 output = ComponentMetaData.OutputCollection.New();
            output.Name = "Output";
            output.SynchronousInputID = input.ID;

            // Get the assembly version and set that as our current version.
            SetComponentVersion();

            ComponentMetaData.OutputCollection[0].ExternalMetadataColumnCollection.IsUsed = true;

            // Insert an error output.
            var errorOutput = ComponentMetaData.OutputCollection.New();
            errorOutput.Name = "ErrorOutput";
            errorOutput.IsErrorOut = true;
            errorOutput.SynchronousInputID = input.ID;
            errorOutput.ExclusionGroup = 1;
        }

        public override IDTSCustomProperty100 SetComponentProperty(string propertyName, object propertyValue)
        {
            if (propertyName == propMappingName)
            {
                this.mappingPaths = JsonParser.GetPropertyList((string)propertyValue);
            }

            return base.SetComponentProperty(propertyName, propertyValue);
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

            return DTSValidationStatus.VS_ISVALID;
        }

        public override void PreExecute()
        {
            // baseclass may need to do some work
            base.PreExecute();
        }

        public override void ProcessInput(int inputID, PipelineBuffer buffer)
        {
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
