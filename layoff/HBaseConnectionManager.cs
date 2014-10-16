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
            LocalizationType = typeof(HBaseConnectionManager),
            DisplayName = "HBase connection manager",
            Description = "HBase connection manager")
    ]
    public class HBaseConnectionManager : ConnectionManagerBase
    {
        private Uri _uri = null;

        public override DTSExecResult Validate(Microsoft.SqlServer.Dts.Runtime.IDTSInfoEvents infoEvents)
        {
            try
            {
                _uri = new Uri(this.Location);
            }
            catch (UriFormatException)
            {
                infoEvents.FireError(0, "HBaseConnectionManager", "Invalid Uri", String.Empty, 0);
                return DTSExecResult.Failure;
            }
            catch (ArgumentNullException)
            {
                infoEvents.FireError(0, "HBaseConnectionManager", "No connection string specified", String.Empty, 0);
                return DTSExecResult.Failure;
            }

            return DTSExecResult.Success;
        }

        public override object AcquireConnection(object txn)
        {
            if (string.IsNullOrEmpty(this.Location))
            {
                return null;
            }

            try
            {
                _uri = new Uri(this.Location);
            }
            catch (UriFormatException)
            {
                return null;
            }
            catch (ArgumentNullException)
            {
                return null;
            }

            string user = "user", pwd = "pwd";
            if (!string.IsNullOrEmpty(_uri.UserInfo))
            {
                var parts = _uri.UserInfo.Split(':');
                if (parts.Length > 1)
                {
                    user = parts[0];
                    pwd = parts[1];
                }
            }

            var creds = new ClusterCredentials(_uri, user, pwd);
            return new HBaseClient(creds)
            {
                RestEndpointBase = string.IsNullOrEmpty(this.RestBasePath) ? null : this.RestBasePath
            };
        }

        public override void ReleaseConnection(object connection)
        {
            if (connection != null && connection as HBaseClient != null)
            {
                // dispose if applicable
            }
        }

        // public override string ConnectionString { get; set; }

        public string Encoding { get; set; }
        public string Location { get; set; }
        public string RestBasePath { get; set; }
    }
}
