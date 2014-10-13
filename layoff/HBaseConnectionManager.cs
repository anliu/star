using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.HBase.Client
{
    [
        DtsConnection(ConnectionType = "HBase",
            DisplayName = "HBase connection manager",
            Description = "HBase connection manager")
    ]
    public class HBaseConnectionManager : ConnectionManagerBase
    {
        public override DTSExecResult Validate(Microsoft.SqlServer.Dts.Runtime.IDTSInfoEvents infoEvents)
        {

            if (String.IsNullOrEmpty(this.ConnectionString))
            {
                infoEvents.FireError(0, "HBaseConnectionManager", "No connection string specified", String.Empty, 0);
                return DTSExecResult.Failure;
            }
            else
            {
                return DTSExecResult.Success;
            }
        }

        public override object AcquireConnection(object txn)
        {
            return ConnectionString;
        }

        public override void ReleaseConnection(object connection)
        {
        }

        public string Credential { get; set; }
    }
}
