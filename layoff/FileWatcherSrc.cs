using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using Star.Layoff.DtsComponents.Common;
using Star.Layoff.DtsComponents.Utilities;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;

namespace Star.Layoff.DtsComponents
{
    [
        DtsPipelineComponent(ComponentType = ComponentType.SourceAdapter,
            CurrentVersion = 4,
            DisplayName = "File Watcher Source",
            Description = "File Watcher Source Component",
            RequiredProductLevel = Microsoft.SqlServer.Dts.Runtime.Wrapper.DTSProductLevel.DTSPL_NONE)
    ]
    public class FileWatcherSrc : PipelineComponent
    {
        private int _outputColIdInBuffer;
        private int _nameColIdInBuffer;
        private int _changeCount;
        private AutoResetEvent _idleEvent;
        private FileSystemWatcher _fileWatcher;
        private ConcurrentQueue<string> _queue;
        //private Event 
        internal string _pathName;
        internal bool Connected { get; set; }

        public override void ProvideComponentProperties()
        {
            // start out clean, remove anything put on by the base class
            RemoveAllInputsOutputsAndCustomProperties();

            // error dispositions
            ComponentMetaData.UsesDispositions = true;

            IDTSRuntimeConnection100 connection = ComponentMetaData.RuntimeConnectionCollection.New();
            connection.Name = "PathName";

            // Add the file path name property.
            //var propPathName = ComponentMetaData.CustomPropertyCollection.New();
            //propPathName.Name = Constants.PropFullPath;
            //propPathName.Description = "Path to monitor";
            //propPathName.ExpressionType = DTSCustomPropertyExpressionType.CPET_NOTIFY;
            //propPathName.Value = string.Empty;

            // Add the columns property.
            var propColumns = ComponentMetaData.CustomPropertyCollection.New();
            propColumns.Name = Constants.PropFilter;
            propColumns.Description = "Filter";
            propColumns.Value = string.Empty;

            IDTSOutput100 output = ComponentMetaData.OutputCollection.New();
            output.Name = "Output";

            // Get the assembly version and set that as our current version.
            SetComponentVersion();

            // we only have fixed meta data
            ComponentMetaData.OutputCollection[0].ExternalMetadataColumnCollection.IsUsed = false;

            // Insert an error output.
            AddErrorOutput("ErrorOutput", 0, 0);

            // Set we don't want to validate external metadata
            ComponentMetaData.ValidateExternalMetadata = false;
        }

        public override void AcquireConnections(object transaction)
        {
            if (ComponentMetaData.RuntimeConnectionCollection[0].ConnectionManager != null)
            {
                ConnectionManager cm = DtsConvert.GetWrapper(ComponentMetaData.RuntimeConnectionCollection[0].ConnectionManager);

                if (cm == null)
                {
                    throw new Exception("The ConnectionManager " + cm.Name + " is not a connection manager.");
                }

                _pathName = (string)cm.AcquireConnection(transaction);
                _queue = new ConcurrentQueue<string>();
                _changeCount = 0;
                _idleEvent = new AutoResetEvent(false);
                _fileWatcher = new FileSystemWatcher(Path.GetFullPath(_pathName));
                // not using Created as Created event was fired before the file
                // content is completedly written, which prevents us reading the
                // file
                //_fileWatcher.Created += (sender, e) =>
                //    {
                //        _queue.Enqueue(e.FullPath);
                //        if (Interlocked.Increment(ref _changeCount) == 1)
                //        {
                //            _idleEvent.Set();
                //        }
                //    };
                _fileWatcher.Renamed += (sender, e) =>
                {
                    _queue.Enqueue(e.FullPath);
                    if (Interlocked.Increment(ref _changeCount) == 1)
                    {
                        _idleEvent.Set();
                    }
                };
                _fileWatcher.EnableRaisingEvents = true;
                this.Connected = true;
            }
        }

        public override void ReleaseConnections()
        {
            // dispose or call CM ReleaseConnections if applicable
            if (_idleEvent != null)
            {
                _idleEvent.Dispose();
                _idleEvent = null;
            }

            if (_fileWatcher != null)
            {
                _fileWatcher.Dispose();
                _fileWatcher = null;
            }
            _pathName = null;
            _queue = null;
            this.Connected = false;
        }

        public override void PreExecute()
        {
            // baseclass may need to do some work
            base.PreExecute();

            // get the non-error output
            int iErrorOutID = 0, iErrorOutIndex = 0;
            GetErrorOutputInfo(ref iErrorOutID, ref iErrorOutIndex);
            var outputMain = ComponentMetaData.OutputCollection[iErrorOutIndex == 0 ? 1 : 0];

            if (!this.Connected)
            {
                bool bCancel;
                ErrorSupport.FireError(HResults.DTS_E_CONNECTIONREQUIREDFORREAD, out bCancel);
                throw new PipelineComponentHResultException(HResults.DTS_E_CONNECTIONREQUIREDFORREAD);
            }

            // there are only two output columns
            // file name column
            var col = outputMain.OutputColumnCollection[0];
            _nameColIdInBuffer = BufferManager.FindColumnByLineageID(outputMain.Buffer, col.LineageID);

            // file content column
            col = outputMain.OutputColumnCollection[1];
            _outputColIdInBuffer = BufferManager.FindColumnByLineageID(outputMain.Buffer, col.LineageID);
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

            // may enter wait
            bufferMain.AddRow();

            bool isEOF = false;
            string oneFile = "__stop";

            while (!isEOF)
            {
                _idleEvent.WaitOne();

                while (_queue.TryDequeue(out oneFile))
                {
                    Interlocked.Decrement(ref _changeCount);

                    if (Path.GetFileName(oneFile) == "__stop")
                    {
                        // special file name to stop
                        isEOF = true;
                        break;
                    }

                    bufferMain.SetString(_nameColIdInBuffer, oneFile);

                    using (var f = File.Open(oneFile, FileMode.Open, FileAccess.Read, FileShare.Read))
                    using (var reader = new BinaryReader(f))
                    {
                        int ioSize = 4096;

                        var bom = reader.ReadBytes(3);
                        if (bom.Length != 3 || (int)bom[0] + (int)(bom[1] << 8) + (int)(bom[2] << 16) != 0xBFBBEF)
                        {
                            // not utf-8 bom, write through the data
                            bufferMain.AddBlobData(_outputColIdInBuffer, bom);
                        }

                        while (true)
                        {
                            var ioData = reader.ReadBytes(ioSize);
                            if (ioData.Length > 0)
                            {
                                bufferMain.AddBlobData(_outputColIdInBuffer, ioData);
                            }

                            if (ioData.Length < ioSize)
                            {
                                break;
                            }
                        }
                    }

                    // may enter wait
                    bufferMain.AddRow();
                }
            }

            // last row
            bufferMain.SetString(_nameColIdInBuffer, oneFile);
            bufferMain.AddBlobData(_outputColIdInBuffer, Encoding.UTF8.GetBytes("{}"));

            // done
            bufferMain.SetEndOfRowset();
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

            int Precision = 0;
            int Scale = 0;
            int CodePage = 0;

            // create file name column
            var outputcolNewMain = outputMain.OutputColumnCollection.NewAt(0);
            var outputcolNewError = outputError.OutputColumnCollection.NewAt(0);

            outputcolNewMain.Name = "FileName";
            outputcolNewError.Name = "FileName";

            // set the output column properties
            outputcolNewMain.SetDataTypeProperties(DataType.DT_WSTR, 1024, Precision, Scale, CodePage);
            outputcolNewError.SetDataTypeProperties(DataType.DT_WSTR, 1024, Precision, Scale, CodePage);

            // set the default error dispositions
            outputcolNewMain.ErrorRowDisposition = DTSRowDisposition.RD_FailComponent;
            outputcolNewMain.TruncationRowDisposition = DTSRowDisposition.RD_FailComponent;
            outputcolNewMain.ErrorOrTruncationOperation = "ErrorOrTruncationOperationConversion";

            // create file content column
            outputcolNewMain = outputMain.OutputColumnCollection.NewAt(1);
            outputcolNewError = outputError.OutputColumnCollection.NewAt(1);

            outputcolNewMain.Name = "FileContent";
            outputcolNewError.Name = "FileContent";

            // set the output column properties
            outputcolNewMain.SetDataTypeProperties(DataType.DT_IMAGE, 0, Precision, Scale, CodePage);
            outputcolNewError.SetDataTypeProperties(DataType.DT_IMAGE, 0, Precision, Scale, CodePage);

            // set the default error dispositions
            outputcolNewMain.ErrorRowDisposition = DTSRowDisposition.RD_FailComponent;
            outputcolNewMain.TruncationRowDisposition = DTSRowDisposition.RD_FailComponent;
            outputcolNewMain.ErrorOrTruncationOperation = "ErrorOrTruncationOperationConversion";

            AddPaddingColumns(outputMain);
        }

        private void AddPaddingColumns(IDTSOutput100 mainOutput)
        {
            var i = mainOutput.OutputColumnCollection.Count;

            // for amd64 OS this is enough to fit one row just above half of the
            // 64k allocation unit
            for (int j = 0; j < 4; j++)
            {
                var outputcolNewMain = mainOutput.OutputColumnCollection.NewAt(i + j);
                outputcolNewMain.Name = string.Format("Padding{0}", j);

                // set the output column properties
                outputcolNewMain.SetDataTypeProperties(DataType.DT_WSTR, 3999, 0, 0, 0);

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
