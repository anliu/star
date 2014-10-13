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
        DtsPipelineComponent(ComponentType = ComponentType.DestinationAdapter,
            DisplayName = "HBase Destination Component",
            Description = "HBase Destination Component")
    ]
    public class HBaseDest : PipelineComponent
    {
    }
}
